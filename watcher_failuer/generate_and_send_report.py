import datetime
import json
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
from pathlib import Path
from scan_scrpy import main as scan_scrpy

path = Path(__file__).parent.absolute()

def get_statistics(db_name, error_message=None):
    statistics = scan_scrpy(db_name, None, True, True, error_message)
    return json.loads(statistics)

def generate_bar_graph(statistics, output_file):
    reasons = list(statistics.keys())

    email_body = "Top 10 Failure Reasons:\n"

    counts = [data['count'] for data in statistics.values()]
    wrapped_reasons = []
    #wrapped_reasons = ['\n'.join(textwrap.wrap(reason, width=40)) for reason in reasons]
    # we won't present the reasons, we will show maps to the reasons
    for i in range(len(reasons)):
        wrapped_reasons.append(i)
        email_body += f"{i}: {reasons[i]}: {counts[i]} occurrences\n"
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


def send_email(to_email, subject, body, attachment_path):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = 'watcher@teuthology.com'
    msg['To'] = to_email
    msg.set_content(body)

    with open(attachment_path, 'rb') as f:
        file_data = f.read()
        file_name = attachment_path

    msg.add_attachment(file_data, maintype='image', subtype='png', filename=file_name)

    smtp_server = 'localhost'
    smtp_port = 25

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as smtp:
            smtp.send_message(msg)
    except Exception as e:
        print(f"Email sending failed: {e}")

def cleanup(keep_db, db_name):
    if not keep_db:
        try:
            subprocess.run(
                ['rm', f'{path}/{db_name}']
            )
        except subprocess.CalledProcessError as e:
            print(f"Error removing database: {e.stderr}")
    try:
        subprocess.run(
            ['rm', f'{path}/failure_statistics.png']
        )
    except subprocess.CalledProcessError as e:
        print(f"Error removing image: {e.stderr}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate and send failure statistics report')
    parser.add_argument('--db_name', required=True, help='The name of the SQLite database')
    parser.add_argument('--email', required=True, help='The email address to send the report to')
    parser.add_argument('--log_directory', help='Directory containing log files to process')
    parser.add_argument('--user_name', help='The user name in directories to scan', default='teuthology')
    parser.add_argument('--days', type=int, help='Number of days to scan back', default=7)
    parser.add_argument('--error_message', help='only find that error message and send the report about that error message by dates', default=None)
    parser.add_argument('--keep_db', help='Keep the database after sending the report', default=False)
    args = parser.parse_args()

    if args.error_message:
        subprocess.run(
            ['python', f'{path}/scan_scrapy_error_message.py', '--log_directory', args.log_directory, '--error_message', args.error_message, '--db_name', args.db_name]
        )
    else:
        if args.log_directory:
            subprocess.run(
                ['python', f'{path}/scan_scrapy_directories.py', '--log_directory', args.log_directory, '--days', str(args.days), '--db_name', args.db_name, '--user_name', args.user_name]
            )

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
        send_email(args.email, subject, email_body, output_image)
        cleanup(args.keep_db, args.db_name)
        exit(0)

    output_image = f"{path}/failure_statistics.png"
    email_body = f"Attached is the failure statistics report fpr the past {args.days} days.\n\n"
    email_body += generate_bar_graph(statistics, output_image)

    send_email(args.email, subject, email_body, output_image)
    cleanup(args.keep_db, args.db_name)
    exit(0)
