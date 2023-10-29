"""
This script will import the congressional voting history, combine it and write to bigquery. 
Steps:
1. Import the data from csvs
2. Combine the data
3. Write to bigquery
"""
# import libraries
import pandas as pd
import requests
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()
import os
from google.oauth2 import service_account
import json
BQ_CREDENTIALS = service_account.Credentials.from_service_account_info(
    json.loads(os.getenv("BIGQUERY_CREDS"))
)

DESTINATION_TABLE = f'congress.voting_history'

import logging
log_level = getattr(logging, os.getenv("LOG_LEVEL"))
logging.basicConfig(
    level=log_level,
    format="%(asctime)s : %(levelname)s : %(funcName)s : %(message)s",
)

def get_members():
    members = pd.read_csv('https://voteview.com/static/data/out/members/HSall_members.csv')
    members = members[['congress', 'chamber', 'icpsr', 'state_abbrev', 'district_code', 'bioname', 'party_code']]

    return members

def get_parties():
    parties = pd.read_csv('https://voteview.com/static/data/out/parties/HSall_parties.csv')
    parties = parties[['congress', 'chamber', 'party_code', 'party_name', 'n_members']]

    return parties

def get_votes():
    votes = pd.read_csv('https://voteview.com/static/data/out/votes/HSall_votes.csv')
    vote_codes = pd.DataFrame(
        data={
            'cast_code':[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'vote_detail':['Not a member of the chamber when this vote was taken', 'Yea', 'Paired Yea', 'Announced Yea', 'Announced Nay', 'Paired Nay', 'Nay', 'Present (some Congresses)','Present (some Congresses)', 'Not Voting (Abstention)'],
            'vote':['n/a', 'Yea', 'Yea', 'Yea', 'Nay', 'Nay', 'Nay', 'Present', 'Present', 'Absent/Not Voting']
        }
    )
    votes = votes.merge(vote_codes, on='cast_code', how='left')

    return votes

def get_bills():
    bills = pd.read_csv('https://voteview.com/static/data/out/rollcalls/HSall_rollcalls.csv')
    bills = bills[['congress', 'chamber', 'rollnumber', 'date', 'bill_number', 'vote_result', 'vote_desc', 'vote_question', 'dtl_desc']]
    return bills


if __name__ == '__main__':
    logging.info('Starting get_congressional_voting_history.py')

    logging.info('Importing members')
    members = get_members()
    logging.info('Importing parties')
    parties = get_parties()

    logging.info('Combining members and parties')
    members_party = members.merge(parties, on=['party_code', 'congress', 'chamber'], how='left')

    logging.info('Importing votes')
    votes = get_votes()

    logging.info('Importing bills')
    bills = get_bills()

    logging.info('Combining votes and bills')
    votes_bills = votes.merge(bills, on=['congress', 'chamber', 'rollnumber'], how='left')
    
    logging.info('Combining members_party and votes_bills')
    all_votes = votes_bills.merge(members_party, on=['congress', 'chamber', 'icpsr'], how='left')

    logging.info('Writing to bigquery')
    all_votes.to_gbq(
        destination_table=DESTINATION_TABLE,
        project_id=os.getenv("BIGQUERY_PROJECT"),
        if_exists='replace',
        credentials=BQ_CREDENTIALS
    )

    logging.info('Finished get_congressional_voting_history.py')