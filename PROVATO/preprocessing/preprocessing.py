from dotenv import load_dotenv
load_dotenv() # load environment variables

import csv, yaml, os, logging, re, json
from datetime import datetime

# # --- TODO ---
# logging.basicConfig(
#     filename = '../../logger.txt',
#     level = logging.INFO,
#     format = '[%(asctime)s] %(levelname)s | Preprocessing | %(message)s',
#     encoding = 'utf-8'
# )

def process_row(row, source, config):
    if check_row_length(row, config) is True:
        return row, {'error': 'check row length'}
    
    row[0] = clean_farm_number(row[0], config)
    row[2] = clean_timedata(row[2], source)
    row[6] = clean_temperature(row[6], config)
    row[7] = clean_humidity(row[7], config)
    row[8] = clean_wind_speed(row[8], config)
    row[9] = clean_wind_direction(row[9])
    row[10] = clean_yetos(row[10], config)
    row[11] = clean_barometer(row[11], config)

    check_cleaned = check_cleaned_row(row)

    if check_cleaned is None:
        return row, {'error': 'check cleaned'}

    return row, {'success': 'ok'}

def check_row_length(row, config):
    return True if len(row) != (len(config['weather_live_basic_data']) + len(config['weather_live_conditions_measurements'])) else False

def clean_timedata(timedata, source):
    try:
        if timedata is None or source is None:
            return None

        cleaned = None

        if source == 'wu':
            timedata = timedata.replace('EEST', '').strip()
            row_time = datetime.strptime(timedata, "%I:%M %p on %B %d, %Y")
            cleaned = row_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif source == 'meteo':
            dt = datetime.strptime(timedata, "%d/%m/%Y %H:%M")
            cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif source == 'soda':
            dt = datetime.strptime(timedata, "%Y-%m-%d %H:%M:%S")
            cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        return cleaned
    except Exception as e:
        # logging.error(f"Error with time converter ({source}): {timedata} -> {e}")
        
        return None

def clean_farm_number(farm, config):
    try:
        if not 'farm' in farm:
            return None

        farm = farm.split('farm')

        if farm is None or not len(farm) == 2 or not 0 <= int(farm[1]) <= len(config['farms']):
            return None

        return farm[1]
    except Exception as e:
        print(e)

        return None

def clean_temperature(temperature, config):
    try:
        units = get_units('temperature', config)

        if temperature is None or units is None:
            return None

        temperature, units = set_lower_and_strip(temperature, units)

        if 'f' in temperature and units == 'c':
            temperature = temperature.replace('f', '').strip()
            temperature = round((float(temperature) - 32) / 1.8, 1)
        elif 'c' in temperature and units == 'c':
            temperature = temperature.replace('°c', '').strip()

        return temperature
    except Exception as e:
        # logging.error(f"Temperature cleaning failed: 'temperature': {temperature} | 'units': {units} -> {e}")
        print(e)
        return None

def clean_humidity(humidity, config):
    try:
        units = get_units('humidity', config)

        if humidity is None or units is None:
            return 'humidity is None'

        humidity, units = set_lower_and_strip(humidity, units)

        if '%' in humidity and units == '%':
            humidity = humidity.replace('%', '').strip()

        return humidity
    except Exception as e:
        # logging.error(f"Humidity cleaning failed: 'humidity': {temperature} | 'units': {units} -> {e}")
        print(e)
        return None

def clean_wind_speed(wind, config):
    try:
        units = get_units('wind', config)
        
        if wind is None or units is None:
            return None

        wind, units = set_lower_and_strip(wind, units)

        if units == 'km/h':
            if 'km/h' in wind:
                wind = float(wind.replace('km/h', '').strip())
            elif 'mph' in wind:
                wind = float(wind.replace('mph', '').strip())
                wind = wind * 1.60934  # mph → km/h
            elif 'm/s' in wind:
                wind = float(wind.replace('m/s', '').strip())
                wind = wind * 3.6      # m/s → km/h
            else:
                wind = float(wind)

        return round(wind, 1)
    except Exception as e:
        print(e)
        return None

def clean_wind_direction(direction):
    try:
        if direction is None:
            return None

        if '°' in direction:
            return direction.replace('°', '').strip()
        
        DIRECTION_TO_DEGREES = {
            'N': 0.0,
            'NNE': 22.5,
            'NE': 45.0,
            'ENE': 67.5,
            'E': 90.0,
            'ESE': 112.5,
            'SE': 135.0,
            'SSE': 157.5,
            'S': 180.0,
            'SSW': 202.5,
            'SW': 225.0,
            'WSW': 247.5,
            'W': 270.0,
            'WNW': 292.5,
            'NW': 315.0,
            'NNW': 337.5,
        }

        match = DIRECTION_TO_DEGREES.get(direction)

        if match is None:
            return None

        return match
    except Exception as e:

        return None

def clean_yetos(yetos, config):
    try:
        units = get_units('yetos', config)
        
        if yetos is None or units is None:
            return None

        yetos, units = set_lower_and_strip(yetos, units)

        if units == 'mm':
            if 'mm' in yetos:
                yetos = float(yetos.replace('mm', '').strip())
            elif 'cm' in yetos:
                yetos = float(yetos.replace('cm', '').strip())
                yetos = yetos * 10  # 1 cm = 10 mm
            elif 'inches' in yetos:
                yetos = float(yetos.replace('inches', '').strip())
                yetos = yetos * 25.4  # 1 inch = 25.4 mm
            elif 'inch' in yetos:
                yetos = float(yetos.replace('inch', '').strip())
                yetos = yetos * 25.4
            elif 'in' in yetos:
                yetos = float(yetos.replace('in', '').strip())
                yetos = yetos * 25.4
            else:
                yetos = float(yetos)

        return round(yetos, 1)
    except Exception as e:

        return None

def clean_barometer(barometer, config):
    try:
        units = get_units('barometer', config)
        
        if barometer is None or units is None:
            return None

        barometer, units = set_lower_and_strip(barometer, units)

        if units == 'hpa':
            if 'hpa' in barometer:
                barometer = float(barometer.replace('hpa', '').strip())
            elif 'mb' in barometer:
                barometer = float(barometer.replace('mb', '').strip())
            elif 'mmhg' in barometer:
                barometer = float(barometer.replace('mmhg', '').strip())
                barometer = barometer * 1.33322
            elif 'inhg' in barometer:
                barometer = float(barometer.replace('inhg', '').strip())
                barometer = barometer * 33.8639
            elif 'in' in barometer:
                barometer = float(barometer.replace('in', '').strip())
                barometer = barometer * 33.8639
            else:
                barometer = float(barometer)

        return round(barometer, 1)
    except Exception as e:
                # logging.error(f"ERROR (preprocessing): clean barometer failed.")

        return None

def check_header(header, config):
    try:
        items = []

        for item in config['weather_live_basic_data']:
            items.append(item)

        for item in config['weather_live_conditions_measurements']:
            items.append(item)

        if not header == items:
            return items

        return header
    except Exception as e:
        print(e)
        return None

def generate_path(path, now, status):
    try:
        year = now.strftime("%Y")
        month = now.strftime("%m")

        if status == 1:
            day = now.strftime("%d")

            directory_path = f"{path}/{year}/{month}/{day}.csv"
        elif status == 2:
            directory_path = f"{path}/{year}/{month}.csv"

        directory = os.path.dirname(directory_path)
        os.makedirs(directory, exist_ok = True)
        file_is_new = not os.path.exists(directory_path) or os.path.getsize(directory_path) == 0

        if file_is_new:
            return True, directory_path 

        return False, directory_path
    except Exception as e:
        print(e)
        return False, None

def set_lower_and_strip(item, unit):
    return item.lower().strip(), unit.lower().strip()

def get_units(item, config):
    return config['results_units'][item]

def load_config():
    with open(os.getenv('CONFIG'), 'r') as conf:
        return yaml.safe_load(conf)

def init_preprocessing():
    try:
        config = load_config()

        for key, value in config['preprocessing'].items():
            staging_path = value['staging']
            cleaned_path = value['cleaned']
            failed_path = value['failed']

            with open(staging_path, 'r', encoding = 'utf-8', newline = '') as staging_file:
                reader = csv.reader(staging_file)
                header = check_header(next(reader, None), config)

                # if header is None or (len(header) != (len(config['weather_live_basic_data'] + config['weather_live_conditions_measurements']))):
                #     # logging.error(f"ERROR (preprocessing): Empty staging file ({key})")
                #     continue

                now = datetime.now()

                check_cleaned, cleaned_path = generate_path(cleaned_path, now, 1)
                check_failed, failed_path = generate_path(failed_path, now, 2)

                with open(failed_path, 'a', encoding = 'utf-8', newline = '') as failed_file, \
                    open(cleaned_path, 'a', encoding = 'utf-8', newline = '') as cleaned_file:

                    if check_cleaned is True:
                        csv.writer(cleaned_file).writerow(check_header(None, config))

                    for row in reader:
                        cleaned_row, status = process_row(row, key, config)

                        if next(iter(status)) == 'error':
                            if check_failed is True:
                                csv.writer(failed_file).writerow(check_header(None, config))

                            # if 'last timedata' in status['error']:
                            #     csv.writer(failed_file).writerow(cleaned_row)

                            csv.writer(failed_file).writerow(row)
                        elif next(iter(status)) == 'success':
                            csv.writer(cleaned_file).writerow(cleaned_row)

                # time.sleep(1.5)

            with open(staging_path, 'w', encoding = 'utf-8', newline = '') as staging:
                csv.writer(staging).writerow(header)
    except Exception as e:

        print(e)

def check_cleaned_row(cleaned_row):
    if 'null' in cleaned_row or None in cleaned_row or '' in cleaned_row:
        return True
    
    # for item in cleaned_row:
    #     if re.search(r'[^0-9\-\:\.]', str(item)):
    #         return True

    for unit in [
                    '°C', 'C', '°F', 'F',
                    '%',
                    'km/h', 'm/s', 'mph', 'bft', 'bf', 'bfr',
                    'mm', 'cm', 'in', 'inch', 'inches',
                    'hpa', 'mb', 'mmHg', 'inHg', 
                    'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW',
                ]:

        if any(unit in str(cell) for cell in cleaned_row):
            return True

    return False

init_preprocessing()
