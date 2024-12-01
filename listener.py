from web3 import Web3
from web3.middleware import geth_poa_middleware  # Necessary for POA chains
import json
from datetime import datetime
import pandas as pd

# Output file for event logs
eventfile = 'deposit_logs.csv'

def scanBlocks(chain, start_block, end_block, contract_address):
    """
    Scans the blockchain for 'Deposit' events emitted by the contract.

    Args:
        chain (str): Blockchain to scan ('avax' or 'bsc').
        start_block (int): Starting block number.
        end_block (int): Ending block number.
        contract_address (str): Address of the deployed contract.

    Writes 'Deposit' events to deposit_logs.csv.
    """
    # Define RPC URLs for supported chains
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"  # Avalanche testnet
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"  # BSC testnet
    else:
        raise ValueError("Unsupported chain. Use 'avax' or 'bsc'.")

    # Initialize Web3 connection
    w3 = Web3(Web3.HTTPProvider(api_url))
    # Inject the middleware for POA chains
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Ensure connection is established
    if not w3.is_connected():
        raise ConnectionError(f"Could not connect to the {chain} chain.")

    # ABI for the 'Deposit' event
    DEPOSIT_ABI = json.loads(
        '[ { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "token", "type": "address" },'
        '{ "indexed": true, "internalType": "address", "name": "recipient", "type": "address" },'
        '{ "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" } ],'
        '"name": "Deposit", "type": "event" }]'
    )

    # Create contract instance
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=DEPOSIT_ABI)

    # Check block range validity
    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"Invalid block range: end_block ({end_block}) < start_block ({start_block})")

    # Log scanning details
    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    # Process blocks
    if end_block - start_block < 30:
        # Handle small block ranges
        event_filter = contract.events.Deposit.create_filter(fromBlock=start_block, toBlock=end_block)
        events = event_filter.get_all_entries()
        process_events(chain, events, contract_address)
    else:
        # Handle large block ranges block-by-block
        for block_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.create_filter(fromBlock=block_num, toBlock=block_num)
            events = event_filter.get_all_entries()
            process_events(chain, events, contract_address)

def process_events(chain, events, contract_address):
    """
    Processes blockchain events and prepares them for logging.

    Args:
        chain (str): Blockchain name ('avax' or 'bsc').
        events (list): List of events from the blockchain.
        contract_address (str): Address of the contract emitting the events.
    """
    logs = []

    for evt in events:
        log = {
            'chain': chain,
            'token': evt.args['token'],
            'recipient': evt.args['recipient'],
            'amount': evt.args['amount'],
            'transactionHash': evt.transactionHash.hex(),
            'address': contract_address,
            'date': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }
        logs.append(log)

    write_to_csv(logs)

def write_to_csv(logs):
    """
    Appends event logs to the CSV file.

    Args:
        logs (list): List of event log dictionaries to write.
    """
    # Convert logs to a DataFrame
    df = pd.DataFrame(logs)

    # Write to the CSV file
    try:
        # Append to the file if it exists
        with open(eventfile, 'a') as f:
            df.to_csv(f, index=False, header=f.tell() == 0, columns=[
                'chain', 'token', 'recipient', 'amount', 'transactionHash', 'address', 'date'
            ])
    except FileNotFoundError:
        # Create the file if it doesn't exist
        df.to_csv(eventfile, index=False, columns=[
            'chain', 'token', 'recipient', 'amount', 'transactionHash', 'address', 'date'
        ])

    print(f"Logged {len(logs)} events to {eventfile}.")
