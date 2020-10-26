import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow.models import Variable

GOOGLE_CREDENTIAL = Variable.get('GOOGLE_CREDENTIALS')

# init client from credentials
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIAL, scope)
client = gspread.authorize(credentials)


def append_to_sheet(sheet_id, worksheet, data):
    """
    Append data to google sheet
    :param sheet_id: Google sheet id.
    :param worksheet: Worksheet name.
    :param data: List of data to be appended.
    """
    sheet = client.open_by_key(sheet_id)
    sheet_instance = sheet.worksheet(worksheet)

    sheet_instance.append_row(data)


def read_sheet(sheet_id, worksheet):
    """
    Read data from google sheet
    :param sheet_id: Google sheet id.
    :param worksheet: Worksheet name.
    :return: Statistics - List of lists.
    """
    sheet = client.open_by_key(sheet_id)
    sheet_instance = sheet.worksheet(worksheet)

    return sheet_instance.get_all_records()


def get_config(**kwargs):
    """
    Get configuration from google sheet
    :kwargs: to get params from previous tasks
    """
    task_instance = kwargs['task_instance']

    # params from dag initialization
    GOOGLE_SHEET_ID = Variable.get("GOOGLE_SHEET_ID")

    worksheet = kwargs['worksheet']
    platform = kwargs['platform']
    run_type = kwargs['type']

    # get config sheet
    config = read_sheet(GOOGLE_SHEET_ID, worksheet)

    # get the platform config and pass to next task
    # through task instance xcom
    for row in config:
        if (row['Platform'] == platform and row['Type'] == run_type):
            task_instance.xcom_push(key='config', value=row)
            break