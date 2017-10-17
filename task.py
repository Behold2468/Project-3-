from urllib.request import urlopen
import time

url = 'https://s3.amazonaws.com/tcmg412-fall2016/http_access_log'
response = urlopen(url)

log_file = response.read().decode('utf-8').split('\n')


# data_dict - dictionary that will be contain count by day for week and month
data_dict = {}
# counter for 4xx status code
status_4xx = 0
# counter for 3xx status code
status_3xx = 0
# request_file - dictionary that will be contain count for unique request file
request_file = {}
# last_month - variable that save last actual month
last_month = 0
# current_file - file variable (for open and write data to file) that allow write data by month
current_file = None
# counter for all log 
total = 0

# for each log in log_file
for log in log_file:
    log = log.split()
    if len(log) == 10:
        # increase total counter
        total += 1
        # parse date 
        date = time.strptime(log[3][1:], '%d/%b/%Y:%H:%M:%S')
        # using date found number of week
        week = int(time.strftime("%U", date))
        
        # start writing log in file by month
        # if current month is equel last
        if date.tm_mon == last_month:
            # write log in current (open) file
            current_file.write(' '.join(log) + '\n')
        # else, if month not equal
        else:
            # close current (last) file
            if current_file:
                current_file.close()
            # open new file with new month
            # mode 'a' - open file to add new data
            current_file = open(time.strftime("%b", date), 'a')
            # say that last_month is equal current month
            last_month = date.tm_mon
        
        # start count log by day for week and month
        # if dictionary contain current month
        if data_dict.get(date.tm_mon):
            # in current month check: if contain current week 
            if data_dict[date.tm_mon].get(week):
                # if yes, check day
                if data_dict[date.tm_mon][week].get(date.tm_mday):
                    # if yes, increase currenr day
                    data_dict[date.tm_mon][week][date.tm_mday] += 1
                # if week not contain current month
                else:
                    # create this day
                    data_dict[date.tm_mon][week][date.tm_mday] = 1
            # if month not contain current week
            else:
                # create week with dictionary: current day (with counter 1)
                data_dict[date.tm_mon][week] = {date.tm_mday: 1}
        # if dictionary not contain current month
        else:
            # create with dictionary current week and day (with counter 1)
            data_dict[date.tm_mon] = {week: {date.tm_mday: 1}}

        # check for 4xx status code
        if 400 <= int(log[8]) < 500:
            # if yes, increase counter
            status_4xx += 1
        
        # check for 3xx status code    
        if 300 <= int(log[8]) < 400:
            # if yes, increase counter
            status_3xx += 1
        
        # check, if request_file contain request file name
        if request_file.get(log[6]):
            # increase counter
            request_file[log[6]] += 1
        # else add this unique request file name
        else:
            # with counter = 1
            request_file[log[6]] = 1

print('Total -', total)
print ()

for month in sorted(data_dict):
    for week in sorted(data_dict[month]):
        for day in sorted(data_dict[month][week]):
            print ('Month:', month, 'day:', day, '-', data_dict[month][week][day])
print ()

for month in sorted(data_dict):
    for week in sorted(data_dict[month]):
        print ('Week:', week, '-', sum(data_dict[month][week].values()))
print ()

for month in sorted(data_dict):
    month_sum = 0
    for week in sorted(data_dict[month]):
        month_sum += sum(data_dict[month][week].values())
    print ('Month:', month, '-', month_sum)
print ()

print ('4xx status code -', status_4xx)
print ('Percentage of the requests were not successful - ', round(status_4xx*100/total, 2), '%')
print ()

print ('3xx status code -', status_3xx)
print ('Percentage of the requests were redirected  - ', round(status_3xx*100/total, 2), '%')
print ()

most_requested_file = sorted(request_file.items(), key=lambda x: x[1], reverse = True)[0][0]
least_requested_file = sorted(request_file.items(), key=lambda x: x[1])[0][0]

print ('Most requested file -', most_requested_file)
print ('Least requested file -', least_requested_file)




