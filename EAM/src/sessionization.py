import sys
import getopt
from datetime import datetime, timedelta


class Session(object):    
    def __init__(self, ip, request_datetime, order):
        self.__ip = ip
        self.__first_request_datetime = request_datetime
        self.__latest_request_datetime = request_datetime
        self.__order = order
        self.__num_of_requests = 0

    
    def __get_ip(self):
        return self.__ip
    IP = property(fget=__get_ip)


    def __get_first_request_datetime(self):
        return self.__first_request_datetime
    FirstRequestDatetime = property(fget=__get_first_request_datetime)


    def __get_latest_request_datetime(self):
        return self.__latest_request_datetime
    def __set_latest_request_datetime(self, value):
        self.__latest_request_datetime = value
    LatestRequestDatetime = property(fget=__get_latest_request_datetime, fset=__set_latest_request_datetime)


    def __get_num_of_requests(self):
        return self.__num_of_requests
    def __set_num_of_requests(self, value):
        self.__num_of_requests = value
    NumOfRequests = property(fget=__get_num_of_requests, fset=__set_num_of_requests)


    def __get_order(self):
        return self.__order
    Order = property(fget=__get_order)


    def __str__(self):
        return "{0},{1},{2},{3},{4}".format(
            self.__ip,
            datetime.strftime(self.__first_request_datetime, "%Y-%m-%d %H:%M:%S"),
            datetime.strftime(self.__latest_request_datetime, "%Y-%m-%d %H:%M:%S"),
            diff_in_seconds(self.__first_request_datetime, self.__latest_request_datetime) + 1,
            self.__num_of_requests)


def create_datetime(datetime_str):
    return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")

def diff_in_seconds(lower, higher):
    return int((higher - lower).total_seconds())

def update_session_info(ip, current_time, first_entry_log_time, session_to_activity_time_index, latest_activity_times, order):
    session = None
    if ip in session_to_activity_time_index:
        idx = session_to_activity_time_index[ip]
        session = latest_activity_times[idx][ip]
        del latest_activity_times[idx][ip]
    else:
        session = Session(ip, current_time, order)
    session.NumOfRequests += 1
    session.LatestRequestDatetime = current_time
    new_idx = diff_in_seconds(first_entry_log_time, current_time)
    session_to_activity_time_index[ip] = new_idx
    latest_activity_times[new_idx][ip] = session

def export_sessions(sessions, session_to_activity_time_index, output_file_name):
    output_file = open(output_file_name, 'a')
    output = []
    for session_groups in sessions:
        for ip, session in session_groups.items():
            output.append(session)
    output.sort(key=lambda x: x.Order)
    for session in output:
        output_file.write(str(session) + '\n')
        del session_to_activity_time_index[session.IP]
    output_file.close()
    

def sessionize(log_file_name, inactivity_file_name, output_file_name):
    open(output_file_name, 'w').close()

    inactivity_period = 0    
    inactivity_file = open(inactivity_file_name)
    try:
        inactivity_period = int(inactivity_file.readline().strip())
    except Exception as ex:
        print "Error in reading inactivity period value from file: {0}".format(ex)
    inactivity_file.close()

    latest_activity_times = [{} for i in range(inactivity_period + 1)]
    session_to_activity_time_index = {}
    first_entry_log_time = None
    previous_entry_log_time = None

    log_file = open(log_file_name)
    log_line = 0
    for log in log_file:
        log_line += 1
        # Skip header
        if log_line == 1:
            continue

        ip, date, time, _, cik, accession, extention, _, _, _, _, _, _, _, _ = log.strip().split(',')
        current_time = create_datetime("{0} {1}".format(date, time))

        if not previous_entry_log_time:
            # First log
            first_entry_log_time = current_time
            session = Session(ip, create_datetime("{0} {1}".format(date, time)), log_line)
            session.NumOfRequests += 1
            session.LatestRequestDatetime = current_time
            latest_activity_times[0][ip] = session
            session_to_activity_time_index[ip] = 0
            previous_entry_log_time = current_time
            continue

        if previous_entry_log_time == current_time or \
            diff_in_seconds(first_entry_log_time, current_time) <= inactivity_period:
            # Either time hasn't advanced or even othe ldest session is not expired. No need to check for expired sessions yet.
            update_session_info(ip, current_time, first_entry_log_time, session_to_activity_time_index, latest_activity_times, log_line)
            previous_entry_log_time = current_time
            continue
        
        # There are expired sessions. Log expired sessions and remove them from lists.
        shift_amount = diff_in_seconds(first_entry_log_time, current_time) - inactivity_period
        export_sessions(latest_activity_times[:shift_amount], session_to_activity_time_index, output_file_name)
        # Update indices.
        latest_activity_times = latest_activity_times[shift_amount:] + [{} for i in range(shift_amount)]
        first_entry_log_time = current_time - timedelta(seconds=inactivity_period)
        # Shift indices in session_to_activity_time_index
        for k, v in session_to_activity_time_index.items():
            session_to_activity_time_index[k] = v - shift_amount
        # Add new session info
        update_session_info(ip, current_time, first_entry_log_time, session_to_activity_time_index, latest_activity_times, log_line)
        previous_entry_log_time = current_time

    log_file.close()
    
    export_sessions(latest_activity_times, session_to_activity_time_index, output_file_name)


if __name__ == "__main__":
    helpMsg = "Usage: python sessionization.py --log=x --inactivity=x --output=x"
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["log=", "inactivity=", "output="])
    except getopt.error, msg:
        print "Error in reading arguments: {0}".format(msg)
        print "for help use --help"
        sys.exit(2)

    log_file_name = "../input/log.csv"
    inactivity_file_name = "../input/inactivity_period.txt"
    output_file_name = "../output/sessionization.txt"
    # process options
    for opt, val in opts:
        if opt == "--log":
            if val.strip(): log_file_name = val.strip()
        if opt == "--inactivity":
            if val.strip(): inactivity_file_name = val.strip()
        if opt == "--output":
            if val.strip(): output_file_name = val.strip()
        elif opt == "--help":
            print helpMsg
            sys.exit(0)

    sessionize(log_file_name, inactivity_file_name, output_file_name)
