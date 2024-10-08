import os
import base64
import re
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import json


# If modifying these scopes, delete the file token.json.
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain new credentials.
    """
    creds = None
    script_dir = os.path.dirname(os.path.abspath(__file__))
    client_secrets_file = os.path.expanduser('~/credentials/client_secret_Google_desktop.json')
    token_file = os.path.join(script_dir, 'token.json')

    # Load existing credentials from token.json if available
    if os.path.exists(token_file):
        with open(token_file, 'r') as token:
            creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)

    # If there are no valid credentials available, prompt the user to log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Failed to refresh credentials: {e}")
                creds = None
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"An error occurred during authentication: {e}")
                return None
        # Save the credentials for the next run
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    return creds


def get_gmail_service(creds):
    """Builds and returns the Gmail API service."""
    try:
        service = build('gmail', 'v1', credentials=creds)
        return service
    except Exception as e:
        print(f"An error occurred while building the Gmail service: {e}")
        return None

def get_sheets_service(creds):
    """Builds and returns the Google Sheets API service."""
    try:
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        print(f"An error occurred while building the Sheets service: {e}")
        return None

def get_medium_digest_email(service, user_email):
    """Fetches the latest email from noreply@medium.com."""
    if not service:
        print("Gmail service is not available.")
        return None
    try:
        results = service.users().messages().list(userId=user_email, q='from:noreply@medium.com').execute()
        messages = results.get('messages', [])

        if not messages:
            print('No emails from noreply@medium.com found.')
            return None

        # Get the latest email.
        message = service.users().messages().get(userId=user_email, id=messages[0]['id'], format='full').execute()
        return message
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def extract_article_links(message):
    """Extracts Medium article links from the email message."""
    links = []
    if 'payload' in message:
        parts = []
        if 'parts' in message['payload']:
            parts = message['payload']['parts']
        else:
            parts = [message['payload']]

        for part in parts:
            if part['mimeType'] == 'text/html':
                data = part['body'].get('data')
                if data:
                    html = base64.urlsafe_b64decode(data).decode('utf-8')
                    soup = BeautifulSoup(html, 'html.parser')
                    all_links = soup.find_all('a', href=True)
                    print(f"Total links found: {len(all_links)}")
                    for a_tag in all_links:
                        href = a_tag['href']
                        print(f"Examining link: {href}")
                        # Use regex to match Medium article URLs
                        match = re.search(r'https://medium\.com/@[^/]+/.+', href)
                        if match:
                            clean_link = match.group(0)
                            if clean_link not in links:
                                links.append(clean_link)
                                print(f"Added article link: {clean_link}")
                        else:
                            print(f"Not a Medium article link: {href}")
    return links

def save_links_to_sheet(sheets_service, spreadsheet_id, links):
    """Saves the list of links to the specified Google Sheet."""
    if not sheets_service:
        print("Sheets service is not available.")
        return

    # Prepare the data to be inserted into the sheet
    values = [[link] for link in links]
    body = {
        'values': values
    }

    try:
        # Clear existing content
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range='A1:Z1000'  # Adjust the range as needed
        ).execute()

        # Write new data
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Successfully saved {len(links)} links to the Google Sheet.")
    except Exception as e:
        print(f"An error occurred while writing to the Google Sheet: {e}")

def main():
    user_email = 'alexander.luzhkov@gmail.com'  # Replace with your email address
    spreadsheet_id = '13bts6J26AhmMlPI2cK1wHVPilAXdTrmHJeFjPG94mio'  # Replace with your Google Sheet ID

    creds = get_credentials()
    if not creds:
        print("Failed to obtain credentials.")
        return

    gmail_service = get_gmail_service(creds)
    sheets_service = get_sheets_service(creds)

    email_message = get_medium_digest_email(gmail_service, user_email)
    if email_message:
        article_links = extract_article_links(email_message)
        if article_links:
            print("\nFound the following Medium article links:")
            for link in article_links:
                print(link)
            # Save links to Google Sheet
            save_links_to_sheet(sheets_service, spreadsheet_id, article_links)
        else:
            print("No Medium article links found in the email.")
    else:
        print("No Medium Daily Digest email found.")

if __name__ == '__main__':
    main()
