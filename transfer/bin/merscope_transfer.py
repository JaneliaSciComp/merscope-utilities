''' merscope_transfer.py
    Transfer MERSCOPE experiment files from local to centralized storage, and optionally
    delete from local storage. Configuration is from a config.json file in the current
    directory.
'''

import argparse
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import os
import shutil
import smtplib
import sys
import time
import colorlog

#pylint: disable=broad-exception-caught
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
SUFFIX = ['analysis', 'output', 'raw_data']
ERRORS = []
DELETED = []
TRANSFERRED = []

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def send_email(mail_text, sender, receivers, subject):
    """ Send an email
        Keyword arguments:
          mail_text: body of email message
          sender: sender address
          receivers: list of recipients
          subject: email subject
          attachment: attachment file name
        Returns:
          None
    """
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ", ".join(receivers)
    message["Subject"] =subject
    message.attach(MIMEText(mail_text, 'plain'))
    try:
        smtpobj = smtplib.SMTP(CONFIG['mail_server'])
        smtpobj.sendmail(sender, receivers, message.as_string())
        smtpobj.quit()
    except smtplib.SMTPException as err:
        raise smtplib.SMTPException("There was a error and the email was not sent:\n" + err)
    except Exception as err:
        raise err


def setup_logging(arg):
    """ Set up colorlog logging
        Keyword arguments:
          arg: argparse arguments
        Returns:
          colorlog handler
    """
    logger = colorlog.getLogger()
    if arg.DEBUG:
        logger.setLevel(colorlog.DEBUG)
    elif arg.VERBOSE:
        logger.setLevel(colorlog.INFO)
    else:
        logger.setLevel(colorlog.WARNING)
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(handler)
    return logger


def experiment_complete(exp):
    """ Ensure that the experiment is complete by the presence of a sentinel
        file that is at least 5 minutes old.
        Keyword arguments:
          base_dir: base directory
        Returns:
          True for complete, False for incomplete
    """
    sentinel = f"{CONFIG['source']}/merfish_raw_data/{exp}/MERLIN_FINISHED"
    if not os.path.isfile(sentinel):
        LOGGER.warning("%s is in process", exp)
        return False
    modtime = os.path.getmtime(sentinel)
    CONFIG['minimum_age'] = 5 * 60
    age = int(time.time() - modtime)
    if age <= CONFIG['minimum_age']:
        hms = datetime.timedelta(seconds=age)
        LOGGER.warning("%s is only %s old", exp, hms)
        return False
    return True


def delete_directory(base_dir):
    """ Delete a directory tree
        Keyword arguments:
          base_dir: base directory
        Returns:
          True for success, False for failure
    """
    if ARG.DELETE:
        try:
            shutil.rmtree(base_dir)
        except Exception as err:
            ERRORS.append(f"Could not rmtree delete {base_dir}\n" \
                          + (TEMPLATE % (type(err).__name__, err.args)))
            return False
        if os.path.exists(base_dir):
            # There are some cases where the tree is deleted with the exception
            # of the top-level dir
            try:
                os.rmdir(base_dir)
            except Exception as err:
                ERRORS.append(f"Could not rmdir delete {base_dir}\n" \
                              + (TEMPLATE % (type(err).__name__, err.args)))
        if os.path.exists(base_dir):
            ERROR.append("Despite attempts to delete it, %s still exists", base_dir)
    LOGGER.warning("Deleted %s", base_dir)
    DELETED.append(base_dir)
    return True


def delete_experiment(exp):
    """ Delete directories for a single experiment
        Keyword arguments:
          exp: experiment
        Returns:
          None
    """
    delete_done = True
    TRANSFERRED.append(exp)
    for sfx in SUFFIX:
        src = f"{CONFIG['source']}/merfish_{sfx}/{exp}"
        if not delete_directory(src):
            delete_done = False
            break
    if delete_done:
        src = f"{CONFIG['secondary']}/{exp}"
        if not os.path.exists(src):
            LOGGER.warning("Secondary delete path %s does not exist", src)
            delete_done = False
        else:
            delete_done = delete_directory(src)
    if not delete_done:
        ERRORS.append(f"Deletion for {exp} is incomplete")


def handle_single_experiment(exp):
    """ Transfer/delete directories for a single experiment
        Keyword arguments:
          exp: experiment
        Returns:
          None
    """
    tgt = CONFIG['secondary']
    if not os.path.exists(tgt):
        ERRORS.append(f"Could not find target path {tgt}")
        return
    missing = False
    for sfx in SUFFIX:
        if not os.path.exists(f"{CONFIG['source']}/merfish_{sfx}/{exp}"):
            missing = True
    if missing:
        LOGGER.debug("%s is not in the required subfolders", exp)
        return
    LOGGER.info(exp)
    if not experiment_complete(exp):
        return
    ok_to_delete = True
    src_base = f"{CONFIG['source']}"
    for sfx in SUFFIX:
        src = f"{src_base}/merfish_{sfx}/{exp}"
        tgt = f"{CONFIG['target']}/merfish_{sfx}/{exp}"
        LOGGER.info("Copy %s to %s", src, tgt)
        if not ARG.TRANSFER:
            continue
        try:
            shutil.copytree(src, tgt, dirs_exist_ok=True)
        except Exception as err:
            ok_to_delete = False
            ERRORS.append(f"Could not copy {src}/{exp}\n"
                          + (TEMPLATE % (type(err).__name__, err.args)))
            break
    if ok_to_delete:
        delete_experiment(exp)


def email_results():
    """ Send a results email message
        Keyword arguments:
          None
        Returns:
          None
    """
    LOGGER.info("Sending mail for transferred/deleted experiments")
    mtext = ""
    if TRANSFERRED:
        mtext += "The following experiments have been transferred:\n"
        if not ARG.TRANSFER:
            mtext += "--- TRANSFER mode was not enabled - no files were transferred ---\n"
        mtext += "\n".join(TRANSFERRED) + "\n\n"
    if DELETED:
        mtext += "The following directories have been deleted:\n"
        if not ARG.DELETE:
            mtext += "--- DELETE mode was not enabled - no files were deleted ---\n"
        mtext += "\n".join(DELETED) + "\n\n"
    if ERRORS:
        mtext += "The following errors have occurred:\n"
        mtext += "\n".join(ERRORS)
    try:
        send_email(mtext, CONFIG['sender'], CONFIG['receivers'],
                   "MERSCOPE experiments transferred")
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))


def process_experiments():
    """ Process the source directories for experiments
        Keyword arguments:
          None
        Returns:
          None
    """
    sdir = f"{CONFIG['source']}/merfish_output"
    LOGGER.info("Reading experiments from %s", sdir)
    try:
        dirs = os.listdir(sdir)
    except FileNotFoundError:
        terminate_program(f"Could not find source directory {sdir}")
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    for edir in dirs:
        handle_single_experiment(edir)
    if TRANSFERRED or DELETED or ERRORS:
        email_results()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Transfer MERSCOPE experiment files")
    PARSER.add_argument('--transfer', dest='TRANSFER', action='store_true',
                        default=False, help='Transfer experiments')
    PARSER.add_argument('--delete', dest='DELETE', action='store_true',
                        default=False, help='Delete experiments')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = setup_logging(ARG)
    with open('config.json', encoding='utf-8') as f:
        CONFIG = json.load(f)
    process_experiments()
    terminate_program()
