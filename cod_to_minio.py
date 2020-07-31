import io
import json
import logging
import os
import re
import sys

from db_utils import minio_utils
import pandas
import requests
import requests_ntlm

REPORT_STORE_HOST = "reportstore.capetown.gov.za"
REPORT_VIEW_PATH = "ReportServer/Pages/ReportViewer.aspx"
REPORT_VIEW_PARAMS = "%2fCOD%2fMail_Rpt&P_SubCouncil=0&P_Ward=0&P_Sector=0&P_Category=0&P_RegOrg=1&P_Members=0&rs%3aFormat=HTML4.0&rs%3aCommand=Render&rc%3aArea=Toolbar&rc%3aToolbar=true"

VIEW_STATE_REGEX = r'id="__VIEWSTATE" value="(\S*)"'

REPORT_VIEW_FORM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "X-MicrosoftAjax": "Delta=true",
    "Cache-Control": "no-cache",
    "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
}
REPORT_VIEW_FORM_DATA = {
    "AjaxScriptManager": "AjaxScriptManager|ReportViewerControl$ctl04$ctl00",
    "__EVENTTARGET": "",
    "__EVENTARGUMENT": "",
    "__VIEWSTATEGENERATOR": "177045DE",
    "NavigationCorrector$ScrollPosition": "",
    "NavigationCorrector$ViewState": "",
    "NavigationCorrector$PageState": "",
    "NavigationCorrector$NewViewState": "",
    "ReportViewerControl$ctl03$ctl00": "",
    "ReportViewerControl$ctl03$ctl01": "",
    "ReportViewerControl$ctl10": "ltr",
    "ReportViewerControl$ctl11": "quirks",
    "ReportViewerControl$AsyncWait$HiddenCancelField": "False",
    "ReportViewerControl$ctl04$ctl03$txtValue": "0",
    "ReportViewerControl$ctl04$ctl05$txtValue": "0",
    "ReportViewerControl$ctl04$ctl07$txtValue": "0",
    "ReportViewerControl$ctl04$ctl09$txtValue": "0",
    "ReportViewerControl$ctl04$ctl11$txtValue": "1",
    "ReportViewerControl$ctl04$ctl13$txtValue": "0",
    "ReportViewerControl$ctl04$ctl15$txtValue": "ORGANISATION  CONTACT DETAILS, MAIN CONTACT PERSON, ALTERNATE CONTACT PERSON, PLANNING DELEGATE, HERITAGE DELEGATE",
    "ReportViewerControl$ctl04$ctl15$divDropDown$ctl01$HiddenIndices": "0,1,2,3,4",
    "ReportViewerControl$ToggleParam$store": "",
    "ReportViewerControl$ToggleParam$collapse": "false",
    "ReportViewerControl$ctl08$ClientClickedId": "",
    "ReportViewerControl$ctl07$store": "",
    "ReportViewerControl$ctl07$collapse": "false",
    "ReportViewerControl$ctl09$VisibilityState$ctl00": "None",
    "ReportViewerControl$ctl09$ScrollPosition": "",
    "ReportViewerControl$ctl09$ReportControl$ctl02": "",
    "ReportViewerControl$ctl09$ReportControl$ctl03": "",
    "ReportViewerControl$ctl09$ReportControl$ctl04": "100",
    "__ASYNCPOST": "true",
    "ReportViewerControl$ctl04$ctl00": "View Report"
}
REPORT_VIEW_FORM_VIEW_STATE_VARIABLE = "__VIEWSTATE"

CONTROL_ID_REGEX = r"ControlID=([a-z0-9]*)"
EXECUTION_ID_REGEX = r"ExecutionID=([a-z0-9]*)"

REPORT_CSV_EXPORT_PATH = "ReportServer/Reserved.ReportViewerWebControl.axd"
REPORT_CSV_EXPORT_PARAM_TEMPLATES = ("ExecutionID={}&Culture=1033&CultureOverrides=False&UICulture=9&"
                                     "UICultureOverrides=False&ReportStack=1&ControlID={}&OpType=Export&"
                                     "FileName=Mail_Rpt&ContentDisposition=OnlyHtmlInline&Format=CSV")

COMMUNITY_ORGANISATION_DATABASE_REPORT_EXPORT_BUCKET = "community-organisation-database.report-export"


def get_city_session(proxy_username, proxy_password, ntlm_username, ntlm_password):
    session = requests.Session()
    session.auth = requests_ntlm.HttpNtlmAuth(ntlm_username, ntlm_password)
    session.proxies = {
        "http": f'http://{proxy_username}:{proxy_password}@internet.capetown.gov.za:8080/',
        "https": f'http://{proxy_username}:{proxy_password}@internet.capetown.gov.za:8080/'
    }

    return session


def get_view_state(session):
    resp = session.get(
        f"http://{REPORT_STORE_HOST}/{REPORT_VIEW_PATH}?{REPORT_VIEW_PARAMS}",
    )
    assert resp.ok, f"View state response from report store not OK! '{resp}: {resp.text}'"

    view_state = re.findall(VIEW_STATE_REGEX, resp.text)[0]
    logging.debug(f"Got the following view state: '{view_state}'")

    return view_state


def trigger_report_generation(session, view_state):
    request_data = {
        **REPORT_VIEW_FORM_DATA,
        REPORT_VIEW_FORM_VIEW_STATE_VARIABLE: view_state
    }

    resp = session.post(
        f"http://{REPORT_STORE_HOST}/{REPORT_VIEW_PATH}?{REPORT_VIEW_PARAMS}",
        headers=REPORT_VIEW_FORM_HEADERS,
        data=request_data
    )

    assert resp.ok, f"Report generation from report store not OK! '{resp}: {resp.text}'"

    control_id = re.findall(CONTROL_ID_REGEX, resp.text)[0]
    execution_id = re.findall(EXECUTION_ID_REGEX, resp.text)[0]
    logging.debug(f"control_id='{control_id}', exeuction_id='{execution_id}'")

    return control_id, execution_id


def get_report_data(session, execution_id, control_id):
    report_csv_params = REPORT_CSV_EXPORT_PARAM_TEMPLATES.format(execution_id, control_id)
    resp = session.get(
        f"http://{REPORT_STORE_HOST}/{REPORT_CSV_EXPORT_PATH}?{report_csv_params}"
    )
    assert resp.ok, f"Report data export from report store not OK! '{resp}: {resp.text}'"

    data = resp.text

    return data


def write_report_data_to_minio(data, minio_access, minio_secret):
    data_df = pandas.read_csv(io.StringIO(data))
    # Fixing this here - strictly speaking should be in the munge step
    data_df.rename(columns={"ï»¿textbox42": "ORG_NAME"}, inplace=True)

    result = minio_utils.dataframe_to_minio(
        data_df,
        minio_bucket=COMMUNITY_ORGANISATION_DATABASE_REPORT_EXPORT_BUCKET,
        minio_key=minio_access,
        minio_secret=minio_secret,
        data_classification=minio_utils.DataClassification.EDGE,
        file_format="parquet"
    )
    assert result, "Write to minio failed"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-%(module)s.%(funcName)s [%(levelname)s]: %(message)s')

    # Loading secrets
    SECRETS_PATH_VAR = "SECRETS_FILE"

    if SECRETS_PATH_VAR not in os.environ:
        logging.error("Secrets path not found!")
        sys.exit(-1)

    secrets_path = os.environ["SECRETS_FILE"]
    secrets = json.load(open(secrets_path))

    logging.info("Generat[ing] HTTP session")
    http_session = get_city_session(secrets["proxy"]["username"], secrets["proxy"]["password"],
                                    secrets["proxy"]["username"], secrets["proxy"]["password"])
    logging.info("Generat[ed] HTTP session")

    logging.info("Gett[ing] view state")
    report_view_state = get_view_state(http_session)
    logging.info("G[ot] view state")

    logging.info("Trigger[ing] report generation")
    report_control_id, report_execution_id = trigger_report_generation(http_session, report_view_state)
    logging.info("Trigger[ed] report generation")

    logging.info("Get[ing] report data")
    report_data = get_report_data(http_session, report_execution_id, report_control_id)
    logging.info("G[ot] report data")

    logging.info("Writ[ing] report data to minio")
    write_report_data_to_minio(report_data, secrets["minio"]["edge"]["access"], secrets["minio"]["edge"]["secret"])
    logging.info("Wr[ote] report data to minio")
