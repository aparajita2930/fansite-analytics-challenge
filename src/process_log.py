import os
import sys
import datetime
from collections import OrderedDict

def write_file(outfile, d, val=None, is_date=None):
    """ This function writes the dictionary to the given output file
    Args:
        outfile: The file to which the output has to be written to
        d: The dictionary which has to be written
        val: Default None; a boolean value to indicate if the values in the dictionary are to be written to the output file
        is_date: Default None; a boolean value to indicate if the keys in the dictionary are datetime values
    Returns:
        None
    """
    d = sorted(d.items(), key=lambda (k, v): (-v, k))
    with open(outfile, 'wb') as f:
        f.write('')
    n = min(len(d), 10)
    for i in range(n):
        with open(outfile, 'ab') as f:
            if val is None:
                f.write('%s\n' % (d[i][0]))
            else:
                if is_date is not None:
                    f.write('%s,%d\n' % (d[i][0].strftime('%d/%b/%Y:%H:%M:%S -0400'), d[i][1]))
                else:
                    f.write('%s,%d\n' % (d[i][0], d[i][1]))

def find_nth_occurence(s, substr, n):
    """ This function finds the nth occurence of a given substring in a given string
    Args:
        s: Given string
        substr: Given substring to be searched for
        n: nth occurence
    Returns:
        Integer: Position of the nth occurence of the substring in the string
    """
    pos = -1
    for x in xrange(n):
        pos = s.find(substr, pos+1)
        if pos == -1:
            return None
    return pos

def fill_datetime(first, last, delta):
    """ This function fills a directory with keys between datetime objects in increment of a time value delta
    Args:
        first: Start datetime of the range
        last: End datetime of the range
        delta: a time value by which to increment the datetimes
    Returns:
        Dictionary: A dictionary containing the datetimes in the given range
    """
    d = OrderedDict()
    curr = first
    while curr <= last:
        d[curr] = 0
        curr += datetime.timedelta(seconds=delta)
    return d

def implement_feature3(lt):
    """ Implement feature 3
    This function is computationally expensive
    Args:
        lt: a list of log times
    Returns:
        Dictionary: A dictionary containing the time window and the count of requests made during that time window
    """
    d = fill_datetime(lt[0], lt[-1], 1)
    for log_time in lt:
        i = 0
        if log_time in d:
            d[log_time] += 1

        keys = d.keys()
        while i < len(d) and (log_time - keys[i]).days * 24 * 60 < 60 and log_time > keys[i]:
            d[d.items()[i][0]] += 1
            i += 1
    return d

def main(infile):
    """ This is the main function
    Args:
        infile: The input log file
    Returns:
        None
    Features:
        Feature 1: List the top 10 most active host/IP addresses that have accessed the site.
        Feature 2: Identify the 10 resources that consume the most bandwidth on the site
        Feature 3: List the top 10 busiest (or most frequently visited) 60-minute periods
        Feature 4: Detect patterns of three failed login attempts from the same IP address over 20 seconds so that all further attempts to the site can be blocked for 5 minutes.
    """
    if len(sys.argv) < 6:
        print 'Usage: python <program-file> <input-file> <out-file1> <out-file2> <out-file3> <out-file4>'
        exit(1)

    with open(sys.argv[5], 'wb') as f:
        f.write('')
    d1 = {}; d2 = {}; d3 = OrderedDict(); d4 = OrderedDict(); lt = []

    with open(infile, 'r') as reader:
        for rows in reader.readlines():
            if rows:
                pos_http = len(rows) - rows[::-1].find('"'); response_bytes = rows[pos_http + 1:].split(' ')
                log_request = rows[rows.find('"'):pos_http]
                log_host = rows[0:rows.find(' - - ')]
                log_time_wzone = rows[rows.find('[') + 1:rows.find(']')]; log_time = datetime.datetime.strptime(log_time_wzone, '%d/%b/%Y:%H:%M:%S -0400')

                log_resource = rows[find_nth_occurence(rows, '/',3):pos_http - 1]
                log_response = response_bytes[0].strip(); log_bytes = response_bytes[1].strip()

                #Create dict for first feature
                if log_host in d1:
                    d1[log_host] += 1
                else:
                    d1[log_host] = 1

                #Create dict for second feature
                val = 0
                if log_bytes.find('-') == -1:
                    val = int(log_bytes)
                log_resource_http = log_resource.find('HTTP/1.0')
                if log_resource_http <> -1:
                    log_resource = log_resource[0:log_resource_http-1]
                else:
                    log_resource = log_resource[0:]
                if log_resource in d2:
                    d2[log_resource] += val
                else:
                    d2[log_resource] = val


                #Create list for third feaure
                lt.append(log_time)

                #Implement 4th feature
                with open(sys.argv[5], 'ab') as f:
                    if log_response == '200' and log_host not in d4:
                        continue
                    elif log_response == '200' and log_host in d4 and ((d4[log_host][0] - log_time).days*24*60*60 > 20):
                        del d4[log_host]
                        continue
                    elif log_response == '200' and log_host in d4 and ((d4[log_host][0] - log_time).days*24*60*60 < 20):
                        if d4[log_host][1] >= 3:
                            f.write('%s - - [%s] "%s" %s %s\n' % (log_host, log_time_wzone, log_request, log_response, log_bytes))
                        continue
                    elif log_response != '200' and log_host in d4 and ((d4[log_host][0] - log_time).days*24*60*60 > 20):
                        del d4[log_host]
                        d4[log_host] = [log_time, 1]
                        continue
                    elif log_response != '200' and log_host not in d4:
                        d4[log_host] = [log_time, 1]
                        continue
                    elif log_response != '200' and log_host in d4 and ((d4[log_host][0] - log_time).days*24*60*60 < 20):
                        retry = d4[log_host][1] + 1
                        d4[log_host][1] = retry
                        if retry > 3:
                            f.write('%s - - [%s] %s %s %s\n' % (log_host, log_time_wzone, log_request, log_response, log_bytes))
                        continue

    write_file(sys.argv[2], d1, True)
    write_file(sys.argv[3], d2)
    d3 = implement_feature3(lt)
    write_file(sys.argv[4], d3, True, True)

if __name__ == '__main__':
    main(sys.argv[1])