from web3 import Web3
from web3.middleware import geth_poa_middleware
import json
from datetime import datetime
import pandas as pd

eventfile = 'deposit_logs.csv'

def scanBlocks(chain, start_block, end_block, contract_address):
    rpc_endpoints = {
        'avax': "https://api.avax-test.network/ext/bc/C/rpc",
        'bsc': "https://data-seed-prebsc-1-s1.binance.org:8545/"
    }

    if chain not in rpc_endpoints:
        raise ValueError("Unsupported chain. Use 'avax' or 'bsc'.")

    w3 = Web3(Web3.HTTPProvider(rpc_endpoints[chain]))
    w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to {chain}.")

    deposit_abi = json.loads('[ { "anonymous": false, "inputs": [ { "indexed": true, "internalType": "address", "name": "token", "type": "address" }, { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" }, { "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Deposit", "type": "event" }]')
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=deposit_abi)

    if start_block == "latest":
        start_block = w3.eth.get_block_number()
    if end_block == "latest":
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"Invalid block range: start_block ({start_block}) > end_block ({end_block})")

    print(f"Scanning blocks {start_block} to {end_block} on {chain}...")

    try:
        pd.read_csv(eventfile)
    except FileNotFoundError:
        pd.DataFrame(columns=['chain', 'token', 'recipient', 'amount', 'transactionHash', 'address', 'date']).to_csv(eventfile, index=False)

    event_rows = []

    if end_block - start_block < 30:
        events = contract.events.Deposit.create_filter(fromBlock=start_block, toBlock=end_block).get_all_entries()
        event_rows.extend(process_events(chain, events, contract_address))
    else:
        for block_num in range(start_block, end_block + 1):
            events = contract.events.Deposit.create_filter(fromBlock=block_num, toBlock=block_num).get_all_entries()
            event_rows.extend(process_events(chain, events, contract_address))

    if event_rows:
        pd.DataFrame(event_rows).to_csv(eventfile, index=False, mode='a', header=False)
        print(f"Scanned and logged {len(event_rows)} events to {eventfile}.")
    else:
        print("No events found in the specified block range.")

def process_events(chain, events, contract_address):
    rows = []
    for event in events:
        rows.append({
            'chain': chain,
            'token': event.args['token'],
            'recipient': event.args['recipient'],
            'amount': event.args['amount'],
            'transactionHash': event.transactionHash.hex(),
            'address': contract_address,
            'date': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        })
    return rows
