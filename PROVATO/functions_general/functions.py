def convert_day(day):  # input: [DD/MM/YY HH:MM] | output: [DD/MM]
    details = day.split(' ')[0].split('/')

    return details[0] + '/' + details[1]

def convert_hour(hour):  # input: [DD/MM/YY HH:MM] | output: [HH:MM]
    return hour.split(' ')[1]
