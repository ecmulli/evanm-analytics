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

# set up environment
from dotenv import load_dotenv
load_dotenv()
import os
from google.oauth2 import service_account
import json
BQ_CREDENTIALS = service_account.Credentials.from_service_account_info(
    json.loads(os.getenv("BIGQUERY_CREDS"))
)

MEMBERS_TABLE_NAME = f'congress.members'
PARTIES_TABLE_NAME = f'congress.parties'
VOTES_TABLE_NAME = f'congress.votes'
BILLS_TABLE_NAME = f'congress.bills'

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
    # log the number of members we are writing to bigquery
    logging.info(f'Writing {len(members)} members to bigquery')
    members.to_gbq(
        destination_table=MEMBERS_TABLE_NAME,
        project_id=os.getenv("BIGQUERY_PROJECT"),
        if_exists='replace',
        credentials=BQ_CREDENTIALS
    )
    logging.info('Importing parties')
    parties = get_parties()
    # log the number of parties we are writing to bigquery
    logging.info(f'Writing {len(parties)} parties to bigquery')
    parties.to_gbq(
        destination_table=PARTIES_TABLE_NAME,
        project_id=os.getenv("BIGQUERY_PROJECT"),
        if_exists='replace',
        credentials=BQ_CREDENTIALS
    )

    logging.info('Importing votes')
    votes = get_votes()
    # log the number of votes we are writing to bigquery
    logging.info(f'Writing {len(votes)} votes to bigquery')
    votes.to_gbq(
        destination_table=VOTES_TABLE_NAME,
        project_id=os.getenv("BIGQUERY_PROJECT"),
        if_exists='replace',
        credentials=BQ_CREDENTIALS
    )

    logging.info('Importing bills')
    bills = get_bills()
    # log the number of bills we are writing to bigquery
    logging.info(f'Writing {len(bills)} bills to bigquery')
    bills.to_gbq(
        destination_table=BILLS_TABLE_NAME,
        project_id=os.getenv("BIGQUERY_PROJECT"),
        if_exists='replace',
        credentials=BQ_CREDENTIALS
    )

    logging.info('Successfully finished get_congressional_voting_history.py')