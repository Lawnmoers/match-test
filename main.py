import asyncio
import aiohttp
import logging
import os  # Import os module
from collections import defaultdict
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Contract address to analyze
CONTRACT_ADDRESS = '0x64812f1212f6276068a0726f4695a6637da3e4f8'.lower()

# MatchScan API base URL
MATCHSCAN_API_URL = 'https://matchscan.io/api'

# Semaphore to limit the number of concurrent tasks
CONCURRENCY_LIMIT = 5  # Adjust based on API rate limits
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Set timeout settings for aiohttp
timeout = aiohttp.ClientTimeout(
    total=300,           # Total timeout of 300 seconds (5 minutes)
    connect=60,          # Time to establish a connection
    sock_connect=60,     # Time for socket to connect
    sock_read=60         # Time for socket to read data
)

# Function selector for send_email
function_selector = '0x5b7d7482'  # Using the method ID you provided
logger.info(f"Function selector for 'send_email': {function_selector}")

# Create 'data' directory if it doesn't exist
data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
    logger.info(f"Created directory '{data_dir}' for data storage.")

async def fetch_transactions(session, action, page, offset, retries=3):
    """
    Fetch transactions involving the contract address with retries.
    """
    params = {
        'module': 'account',
        'action': action,
        'address': CONTRACT_ADDRESS,
        'page': page,
        'offset': offset,
        'sort': 'asc'
    }
    headers = {
        'accept': 'application/json'
    }
    attempt = 0
    while attempt < retries:
        async with semaphore:
            try:
                async with session.get(MATCHSCAN_API_URL, params=params, headers=headers) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"Error fetching {action}: HTTP {response.status} - {text}")
                        if response.status == 502:
                            attempt += 1
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        return None
                    data = await response.json()
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Error fetching {action}: {e}")
                attempt += 1
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    logger.error(f"Failed to fetch {action} after {retries} retries.")
    return None

async def fetch_transaction_details(session, tx_hash, retries=3):
    """
    Fetch detailed information for a single transaction.
    """
    params = {
        'module': 'transaction',
        'action': 'gettxinfo',
        'txhash': tx_hash
    }
    headers = {
        'accept': 'application/json'
    }
    attempt = 0
    while attempt < retries:
        async with semaphore:
            try:
                async with session.get(MATCHSCAN_API_URL, params=params, headers=headers) as response:
                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"Error fetching transaction details for {tx_hash}: HTTP {response.status} - {text}")
                        if response.status == 502:
                            attempt += 1
                            await asyncio.sleep(2 ** attempt)
                            continue
                        return None
                    data = await response.json()
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f"Error fetching transaction details for {tx_hash}: {e}")
                attempt += 1
                await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch transaction details for {tx_hash} after {retries} retries.")
    return None

async def process_transactions(session, action, transactions_list, counters):
    """
    Fetch and process transactions for the given action.
    """
    page = 1
    offset = 100  # Number of transactions per page (adjust if needed)
    tasks = []
    while True:
        data = await fetch_transactions(session, action, page, offset)
        if not data:
            logger.error(f"Skipping {action} due to repeated errors.")
            break
        if data.get('status') != '1':
            message = data.get('message', '')
            if message == 'No transactions found':
                logger.info(f"No more transactions found for {action} on page {page}.")
                break
            else:
                logger.error(f"Error fetching transactions for {action}: {message}")
                break

        transactions = data.get('result', [])
        if not transactions:
            logger.info(f"No more transactions found for {action} on page {page}.")
            break

        counters['total_txs_fetched'] += len(transactions)

        for tx in transactions:
            tx_hash = tx.get('hash')
            tx_from = tx.get('from', '').lower()
            tx_time = int(tx.get('timeStamp', '0'))
            tx_datetime = datetime.fromtimestamp(tx_time)

            # Check transaction status
            is_error = tx.get('isError', '0')  # '0' means success, '1' means failure
            tx_receipt_status = tx.get('txreceipt_status', '1')  # '1' means success, '0' means failure

            status = 'Success'
            if is_error == '1' or tx_receipt_status == '0':
                status = 'Failed'

            # Only consider successful transactions
            if status == 'Success':
                # Fetch transaction details to get the input data
                task = asyncio.create_task(process_transaction_detail(session, tx_hash, tx_from, tx_datetime, transactions_list, counters))
                tasks.append(task)

        logger.info(f"Processed page {page} for {action}, transactions fetched: {len(transactions)}")
        page += 1

    if tasks:
        await asyncio.gather(*tasks)

async def process_transaction_detail(session, tx_hash, tx_from, tx_datetime, transactions_list, counters):
    """
    Fetch transaction details and check if it calls the send_email function.
    """
    counters['total_txs_processed'] += 1
    tx_details = await fetch_transaction_details(session, tx_hash)
    if tx_details and tx_details.get('status') == '1':
        tx_input = tx_details['result'].get('input', '')
        if tx_input.startswith(function_selector):
            transactions_list.append({
                'hash': tx_hash,
                'from': tx_from,
                'time': tx_datetime
            })
            counters['total_txs_matching'] += 1
    else:
        logger.error(f"Could not fetch details for transaction {tx_hash}")

async def main():
    transactions_list = []
    counters = {
        'total_txs_fetched': 0,
        'total_txs_processed': 0,
        'total_txs_matching': 0
    }

    async with aiohttp.ClientSession(timeout=timeout) as session:
        logger.info(f"Starting to fetch transactions for contract {CONTRACT_ADDRESS}")

        actions = ['txlist']  # Assuming normal transactions represent emails sent

        for action in actions:
            await process_transactions(session, action, transactions_list, counters)

    if not transactions_list:
        logger.warning("No transactions found for the contract. Please verify the contract address and its activity.")
        return

    # Log the total number of transactions
    logger.info(f"\nTotal transactions fetched: {counters['total_txs_fetched']}")
    logger.info(f"Total successful transactions processed: {counters['total_txs_processed']}")
    logger.info(f"Total transactions calling send_email: {counters['total_txs_matching']}")

    # Save counters to a file in the 'data' directory
    with open(os.path.join(data_dir, 'counters.txt'), 'w') as f:
        f.write(f"Total transactions fetched: {counters['total_txs_fetched']}\n")
        f.write(f"Total successful transactions processed: {counters['total_txs_processed']}\n")
        f.write(f"Total transactions calling send_email: {counters['total_txs_matching']}\n")
    logger.info("Counters have been saved to 'data/counters.txt'.")

    # Save all matching transactions to a file in the 'data' directory
    with open(os.path.join(data_dir, 'transactions.txt'), 'w') as f:
        for tx in transactions_list:
            f.write(f"TxHash: {tx['hash']}, From: {tx['from']}, Time: {tx['time']}\n")
    logger.info("Transactions have been saved to 'data/transactions.txt'.")

    # Analyze campaign effectiveness
    user_activity = defaultdict(list)  # {user_address: [list of dates]}
    daily_emails_sent = defaultdict(int)  # {date: total_emails_sent}
    daily_active_users = defaultdict(set)  # {date: set of active users}

    for tx in transactions_list:
        user = tx['from']
        date = tx['time'].date()
        user_activity[user].append(date)
        daily_emails_sent[date] += 1
        daily_active_users[date].add(user)

    # Calculate daily active users (DAU)
    dau = {date: len(users) for date, users in daily_active_users.items()}

    # Calculate user retention and participation
    total_users = len(user_activity)
    participation_days = [len(set(dates)) for dates in user_activity.values()]
    average_participation = sum(participation_days) / total_users if total_users > 0 else 0

    # Identify users who sent emails every day
    if daily_emails_sent:
        campaign_start_date = min(daily_emails_sent.keys())
        campaign_end_date = max(daily_emails_sent.keys())
        campaign_duration = (campaign_end_date - campaign_start_date).days + 1
        consistent_users = [user for user, dates in user_activity.items() if len(set(dates)) == campaign_duration]
    else:
        campaign_start_date = campaign_end_date = datetime.now().date()
        campaign_duration = 0
        consistent_users = []

    logger.info(f"\nTotal users participated: {total_users}")
    logger.info(f"Average participation days per user: {average_participation:.2f}")
    logger.info(f"Users who sent emails every day: {len(consistent_users)}")

    # Save user participation data to 'data' directory
    with open(os.path.join(data_dir, 'user_participation.txt'), 'w') as f:
        for user, dates in user_activity.items():
            f.write(f"User: {user}, Days Participated: {len(set(dates))}\n")
    logger.info("User participation data has been saved to 'data/user_participation.txt'.")

    # Identify top engaged users
    top_users = sorted(user_activity.items(), key=lambda x: len(set(x[1])), reverse=True)[:10]
    logger.info("\nTop engaged users (by number of days participated):")
    for user, dates in top_users:
        logger.info(f"User: {user}, Days Participated: {len(set(dates))}")

    # Save top engaged users to a file
    with open(os.path.join(data_dir, 'top_engaged_users.txt'), 'w') as f:
        for user, dates in top_users:
            f.write(f"User: {user}, Days Participated: {len(set(dates))}\n")
    logger.info("Top engaged users have been saved to 'data/top_engaged_users.txt'.")

    # Generate graphs
    if daily_emails_sent:
        # Emails sent per day
        dates_sorted = sorted(daily_emails_sent.keys())
        emails_sent = [daily_emails_sent[date] for date in dates_sorted]
        dau_values = [dau[date] for date in dates_sorted]

        plt.figure(figsize=(12, 6))
        plt.bar(dates_sorted, emails_sent, color='blue', alpha=0.7, label='Emails Sent')
        plt.plot(dates_sorted, dau_values, color='red', marker='o', label='Daily Active Users')
        plt.title('Emails Sent and Daily Active Users Over Time')
        plt.xlabel('Date')
        plt.ylabel('Count')
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(data_dir, 'emails_sent_and_dau.png'))
        plt.close()
        logger.info("Graph 'emails_sent_and_dau.png' has been saved to 'data' directory.")

        # User retention over time
        retention = {}
        for day in range(campaign_duration):
            date = campaign_start_date + timedelta(days=day)
            if day == 0:
                initial_users = daily_active_users.get(date, set())
                retention[date] = len(initial_users)
            else:
                prev_date = campaign_start_date + timedelta(days=day - 1)
                prev_users = daily_active_users.get(prev_date, set())
                current_users = daily_active_users.get(date, set())
                retained_users = prev_users & current_users
                retention[date] = len(retained_users)

        retention_dates = sorted(retention.keys())
        retention_values = [retention[date] for date in retention_dates]

        plt.figure(figsize=(12, 6))
        plt.plot(retention_dates, retention_values, marker='o', color='green')
        plt.title('User Retention Over Time')
        plt.xlabel('Date')
        plt.ylabel('Number of Retained Users')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(data_dir, 'user_retention.png'))
        plt.close()
        logger.info("Graph 'user_retention.png' has been saved to 'data' directory.")
    else:
        logger.info("No data available to generate graphs.")

if __name__ == '__main__':
    asyncio.run(main())
