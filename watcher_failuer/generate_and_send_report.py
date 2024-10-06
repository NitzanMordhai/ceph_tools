import datetime
import json
import os
import subprocess
import argparse
import smtplib
from email.message import EmailMessage
import matplotlib.pyplot as plt
import textwrap
from collections import defaultdict
import numpy as np
from matplotlib import cm
import re
import glob
from pathlib import Path
from scan_scrpy import main as scan_scrpy
from scan_scrapy_error_message import main as scan_scrapy_error_message
from scan_scrapy_directories import main as scan_scrapy_directories
from trackers import RedmineConnector

path = Path(__file__).parent.absolute()
_verbose = False

def get_statistics(db_name, error_message=None):
    statistics = scan_scrpy(db_name, None, True, True, error_message)
    return json.loads(statistics)

def generate_bar_graph(statistics, output_file):
    reasons = list(statistics.keys())
    tracker = RedmineConnector()
    email_body = "Top 10 Failure Reasons:\n"

    counts = [data['count'] for data in statistics.values()]
    wrapped_reasons = []
    #wrapped_reasons = ['\n'.join(textwrap.wrap(reason, width=40)) for reason in reasons]
    # we won't present the reasons, we will show maps to the reasons
    for i in range(len(reasons)):
        wrapped_reasons.append(i)
        res = tracker.search_and_refine(reasons[i])
        if len(res) != 0:
            print(f"Reason: {res} was {reasons[i]}")
            reasons[i] = res.get('link')
        email_body += f"{i+1}: {reasons[i]}: {counts[i]} occurrences\n"
    plt.figure(figsize=(15, 15))
    bars = plt.barh(wrapped_reasons, counts, color='skyblue')
    plt.xlabel('Number of Occurrences')
    plt.title('Top 10 Failure Reasons')
    
    for bar in bars:
        plt.text(bar.get_width(), bar.get_y() + bar.get_height() / 2, f' {bar.get_width()}', va='center', ha='left')


    plt.ylabel('Failure Reason')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig(output_file)
    return email_body

def generate_error_message_line_plot(statistics, output_file):
    directory_data = defaultdict(lambda: {"count": 0, "job_ids": []})

    for data in statistics.values():
        directory = data.get("directory", "unknown")
        job_ids = data.get("job_ids", [])
        
        directory_data[directory]["count"] = len(job_ids)
        directory_data[directory]["job_ids"].extend(job_ids)

    # Extract date and time from directory names
    sorted_directories = sorted(directory_data.keys())
    date_times = [extract_date_time(dir) for dir in sorted_directories]
    
    # Prepare data for plotting
    counts = [directory_data[dir]["count"] for dir in sorted_directories]
    
    # Convert directory names to numerical indices for the x-axis
    indices = list(range(len(sorted_directories)))
    
    # Create a line plot
    plt.figure(figsize=(60, 50), dpi=100)
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.3)  # Adjust bottom margin for rotated labels
    
    plt.plot(indices, counts, 'o-', color='blue', label='Count')  # Use a single line for counts

    # Add markers for each date-time
    for idx, date_time in enumerate(date_times):
        plt.text(idx, counts[idx], f'{counts[idx]}', ha='center', va='bottom', fontsize=16, color='black')

    # Create a legend
    plt.legend(title="Counts")
    
    # Set x-ticks to date-times with rotation
    plt.xticks(indices, date_times, rotation=45, ha='right', fontsize=16)
    
    plt.xlabel('Date and Time')
    plt.ylabel('Number of Occurrences')
    plt.title('Line Plot of Job IDs by Date and Time')
    
    # Improve layout to accommodate rotated labels
    plt.tight_layout(pad=1.0, rect=[0, 0, 1, 0.95])
    plt.savefig(output_file)
    plt.close()
    
def extract_date_time(directory):
    # Regular expression to match the date and time part
    match = re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2})', directory)
    return match.group(0) if match else 'unknown'


def prepare_email_message(subject, body, to_email, attachment_path):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = 'watcher@teuthology.com'
    msg['To'] = to_email
    msg.set_content(body)

    with open(attachment_path, 'rb') as f:
        file_data = f.read()
        file_name = attachment_path

    msg.add_attachment(file_data, maintype='image', subtype='png', filename=file_name)

    send_email(msg)

def cleanup(keep_db, db_name, image):
    # ignore the error if the file does not exist
    try:
        os.remove(f'{path}/{db_name}')
    except FileNotFoundError:
        pass
    
    try:
        os.remove(f'{path}/{image}')
    except FileNotFoundError:
        pass

def get_all_versions(log_directory, days, db_name, user_name, branch_name, suite_name, to_email):
    versions = ['octopus', 'pacific', 'quincy', 'squid', 'main']
    flavors = ['default', 'crimson']
    email_body = ""
    scan_report_start_date = datetime.date.today() - datetime.timedelta(days=days)
    scan_report_end_date = datetime.date.today()
    subject = f"Failure Statistics Report for {scan_report_start_date} to {scan_report_end_date}"
    # create email body with html format and each version has its own section
    for version in versions:
        if branch_name != '':
            _branch_name = f'{branch_name}-{version}'
        else:
            _branch_name = f'{version}'
        for flavor in flavors:
            db_name_v = f'{db_name}_{version}_{flavor}'
            dir_results = scan_scrapy_directories(log_directory, days, db_name_v, user_name, suite_name, _branch_name, flavor, _verbose)
            statistics = get_statistics(db_name_v, None)
            if statistics == {}: # skip if no data
                continue
            email_body += "Directories scanned for version: " + version + "\\" + flavor + "\n"
            for result in dir_results:
                email_body += f"   {result}\n"
            email_body += generate_bar_graph(statistics, f"{path}/{version}_{flavor}_failure_statistics.png")
            email_body += "\n\n"
    prepare_email_message_versions(subject, email_body, to_email, path)
    for version in versions:
        cleanup(False, db_name_v, f'{version}_failure_statistics.png')

def get_all_bot_results(log_directory, days, db_name, user_name, branch_name, suite_name, flavor, to_email):
    users = ['yuriw', 'teuthology']
    for user in users:
        if user == 'yuriw':
            branch_name = 'wip-yuri*-testing-*'
        elif user == 'teuthology':
            branch_name = ''
        else:
            print("Unknown user")
            exit(1)
        print(f"Processing user: {user}")
        get_all_versions(log_directory, days, db_name, user, branch_name, suite_name, to_email)

def prepare_email_message_versions(subject, body, to_email, attachment_path):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = 'watcher@teuthology.com'
    msg['To'] = to_email
    msg.set_content(body)
    
    # browse the attachment path and add the image to the email
    directories = glob.glob(f"{attachment_path}/*_failure_statistics.png")
    for directory in directories:
        with open(directory, 'rb') as f:
            file_data = f.read()
            file_name = directory
        print(f"Adding attachment: {file_name}")
        msg.add_attachment(file_data, maintype='image', subtype='png', filename=file_name)
    
    send_email(msg)
        
def send_email(email_msg):
    smtp_server = 'localhost'
    smtp_port = 25
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as smtp:
            smtp.send_message(email_msg)
    except Exception as e:
        print(f"Email sending failed: {e}")
    finally:
        print("Email sent successfully")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate and send failure statistics report')
    parser.add_argument('--db_name', required=True, help='The name of the SQLite database')
    parser.add_argument('--email', required=True, help='The email address to send the report to')
    parser.add_argument('--log_directory', help='Directory containing log files to process')
    parser.add_argument('--all_versions', help='Process all versions (ignoring branch name and using yuri test pattern)', default=False)
    parser.add_argument('--user_name', help='The user name in directories to scan', default='yuriw')
    parser.add_argument('--suite_name', help='The suite name in directories to scan', default='rados')
    parser.add_argument('--branch_name', help='The branch name in directories to scan', default='wip-yuri*-testing-*')
    parser.add_argument('--flavor', type=str, help='The flavor in directories to scan', default='default')
    parser.add_argument('--days', type=int, help='Number of days to scan back', default=7)
    parser.add_argument('--error_message', help='only find that error message and send the report about that error message by dates', default=None)
    parser.add_argument('--keep_db', help='Keep the database after sending the report', default=False)
    parser.add_argument('--bot', help='Run as a bot results', action='store_true')
    parser.add_argument('--verbose', help='Print verbose output', action='store_true')
    args = parser.parse_args()
    _verbose = args.verbose
    if args.bot:
        print("Processing bot results")
        get_all_bot_results(args.log_directory, args.days, args.db_name, args.user_name, args.branch_name, args.suite_name, args.flavor, args.email)
        exit(0)

    if args.all_versions:
        get_all_versions(args.log_directory, args.days, args.db_name, args.user_name, args.branch_name, args.suite_name, args.email)
        exit(0)

    if args.error_message:
        dir_results = scan_scrapy_error_message(args.log_directory, args.date, args.db_name, args.error_message, args.user_name, args.flavor)
    else:
        if args.log_directory:
            dir_results = scan_scrapy_directories(args.log_directory, args.days, args.db_name, args.user_name, args.suite_name, args.branch_name, args.flavor)

    statistics = get_statistics(args.db_name, args.error_message)
    email_body = ""
    # send emails to: Kamoltat Sirivadhna <ksirivad@redhat.com>, Neha Ojha <nojha@redhat.com>, Radoslaw Zarzynski <rzarzyns@redhat.com>, Laura Flores <lflores@redhat.com>, Nitzan Mordechai <nmordech@redhat.com>, Yaarit Hatuka <yhatuka@redhat.com>
    #args.email = "Kamoltat Sirivadhna <ksirivad@redhat.com>, Neha Ojha <nojha@redhat.com>, Radoslaw Zarzynski <rzarzyns@redhat.com>, Laura Flores <lflores@redhat.com>, Nitzan Mordechai <nmordech@redhat.com>, Yaarit Hatuka <yhatuka@redhat.com>"
    scan_report_start_date = datetime.date.today() - datetime.timedelta(days=args.days)
    scan_report_end_date = datetime.date.today()
    subject = f"Failure Statistics Report for {scan_report_start_date} to {scan_report_end_date}"
    
    if args.error_message:
        output_image = f"{path}/{args.error_message}_failure_statistics.png"
        generate_error_message_line_plot(statistics, output_image)
        email_body = f"Attached is the failure statistics report for the error message: {args.error_message}"
        prepare_email_message(subject, email_body, args.email, output_image)
        cleanup(args.keep_db, args.db_name, f"{args.error_message}_failure_statistics.png")
        exit(0)

    output_image = f"{path}/failure_statistics.png"
    email_body = f"Attached is the failure statistics report for the past {args.days} days.\n\n"
    email_body += "Directories scanned:\n"
    for result in dir_results:
        email_body += f"   {result}\n"
    email_body += generate_bar_graph(statistics, output_image)

    prepare_email_message(subject, email_body, args.email, output_image)
    cleanup(args.keep_db, args.db_name, 'failure_statistics.png')
    exit(0)
