from bs4 import BeautifulSoup
from glob import iglob
from os import path
import random
import re
import requests
import time

# See https://gist.github.com/dideler/5219706#file-extract_emails_from_text-py
emailRegex = re.compile("([a-z0-9!#$%&*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                        "{|}~-]+)*(@)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\.))+[a-z0-9]"
                        "(?:[a-z0-9-]*[a-z0-9])?)|([a-z0-9!#$%&*+\/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+\/=?^_`"
                        "{|}~-]+)*(\sat\s)(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(\sdot\s))+[a-z0-9]"
                        "(?:[a-z0-9-]*[a-z0-9])?)", re.S)

searchTerms = ['lncrna', 'circular rna', 'circrna',
               'microrna', 'mirna',  'lincrna', 'mrna']


def getEmails(text_content: str) -> list:
    """Returns a list of matched emails found in `text_content`."""
    # Removing lines that start with '//' because the regular expression
    # mistakenly matches patterns like 'http://foo@bar.com' as '//foo@bar.com'.
    return [email[0] for email in re.findall(emailRegex, text_content) if not email[0].startswith('//')]


def log(text):
    print(f'{time.strftime("%Y-%m-%d %H:%M:%S")} | {text}')


def searchPubmed(yrFrom: int = 2019, yrTo: int = 2022):
    searchTerm_i, page = 0, 1
    checkpoint_file = './checkpoint.txt'
    if path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint = f.read()
        if not checkpoint:
            return

        splitted = [int(c) for c in checkpoint.split(',')]
        searchTerm_i = splitted[0]
        page = splitted[1]

    PUBMED_URL_PART = f'https://pubmed.ncbi.nlm.nih.gov/?size=200&filter=simsearch2.ffrft&filter=years.{yrFrom}-{yrTo}&format=pubmed'

    for i in range(searchTerm_i, len(searchTerms)):
        searchTerm = searchTerms[i].replace(' ', '+')
        log(f'Processing search item {i}: {searchTerm}. Page: {page}')
        url = PUBMED_URL_PART + '&term=' + searchTerm
        fails = 0
        while True:
            if fails > 5:
                # Break so we can try the next search item
                page = 1
                with open(checkpoint_file, 'w') as f:
                    f.write(f'{i+1},{page}')
                log(f'Processing search item {i}: {searchTerm}. Page: {page}. Too many fails. Breaking out')
                break

            resp = requests.get(url + f'&page={page}')
            if page % 50 == 0:
                log(f'Processing search item {i}: {searchTerm}. Page: {page}')
            time.sleep(random.uniform(0.01, 0.35))
            page += 1
            if resp.status_code != 200:
                fails += 1
                log(f'FAILURE: {searchTerm}. Page: {page}. Status: {resp.status_code}. Failure #: {fails}')
                continue

            fails = 0
            soup = BeautifulSoup(resp.text, 'html.parser')

            content = soup.find('pre')
            if not content:
                # Break since it appears we've reached the end of valid pages
                page = 1
                with open(checkpoint_file, 'w') as f:
                    f.write(f'{i+1},{page}')
                log(f'Processing search item {i}: {searchTerm}. Page: {page}. No more results. Breaking out')
                break
            
            emails = getEmails(str(content))
            if len(emails) > 0:
                with open(f'./{searchTerm}.emails.txt', 'a') as f:
                    f.write((','.join(emails) + ','))

            # Checkpoint
            with open(checkpoint_file, 'w') as f:
                f.write(f'{i},{page}')

            if (page - 1) % 50 == 0:
                log(f'Processing search item {i}: {searchTerm}. Page: {page}. {len(emailsAsList)} unique emails so far')

        # Store only unique emails
        with open(f'./{searchTerm}.emails.txt', 'r') as f:
            emailsOnFile = f.read()
        emailsAsList = set([e.strip() for e in emailsOnFile.split(',') if e and e.strip()])
        with open(f'./{searchTerm}.emails.txt', 'w') as f:
            f.write(','.join(emailsAsList))

        log(f'[{searchTerm}] Finished. Checked {page} pages of 200 items each. {len(emailsAsList)} unique emails found')

    # Checkpoint emptied to mean DONE
    with open(checkpoint_file, 'w') as f:
        f.write('')


def collateEmails():
    allEmailsFile = './all-emails.txt'
    if not path.exists(allEmailsFile):
        for emailFile in iglob('./*.emails.txt'):
            with open(emailFile, 'r') as f:
                emailsOnFile = f.read()
            with open(allEmailsFile, 'a') as f:
                f.write((emailsOnFile + ','))

    with open(allEmailsFile, 'r') as f:
        emailsOnFile = f.read()
    emailsAsList = set([e.strip() for e in emailsOnFile.split(',') if e and e.strip()])
    log(f'All emails - {len(emailsAsList)} unique emails')
    with open(allEmailsFile, 'w') as f:
        f.write(','.join(emailsAsList))


if __name__ == '__main__':
    searchPubmed()
    collateEmails()
