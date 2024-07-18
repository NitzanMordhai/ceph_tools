import datetime
import json
import subprocess
import argparse
import smtplib
from email.message import EmailMessage
import matplotlib.pyplot as plt
import textwrap

def get_statistics(db_name):
    result = subprocess.run(
        ['python', 'scan_scrpy.py', '--db_name', db_name, '--get_statistics', '--json'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"scan_scrpy.py output: {result.stdout}")
        raise Exception(f"Error in scan_scrpy.py: {result.stderr}")
    return json.loads(result.stdout)

def generate_bar_graph(statistics, output_file):
    reasons = list(statistics.keys())
    counts = [data['count'] for data in statistics.values()]
    wrapped_reasons = ['\n'.join(textwrap.wrap(reason, width=40)) for reason in reasons]

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate and send failure statistics report')
    parser.add_argument('--db_name', required=True, help='The name of the SQLite database')
    parser.add_argument('--email', required=True, help='The email address to send the report to')
    parser.add_argument('--log_directory', help='Directory containing log files to process')
    parser.add_argument('--days', type=int, help='Number of days to scan back', default=7)
    args = parser.parse_args()

    if args.log_directory:
        subprocess.run(
            ['python', 'scan_scrapy_directories.py', '--log_directory', args.log_directory, '--days', str(args.days), '--db_name', args.db_name]
        )

    try:
        statistics = get_statistics(args.db_name)
    except Exception as e:
        print(f"Failed to get statistics: {e}")
        exit(1)

    output_image = 'failure_statistics.png'
    generate_bar_graph(statistics, output_image)
    email_body = "Attached is the failure statistics report fpr the past {days} days.".format(days=50)
    subject = f"Failure Statistics Report {datetime.datetime.now()}"
    send_email(args.email, subject, email_body, output_image)
