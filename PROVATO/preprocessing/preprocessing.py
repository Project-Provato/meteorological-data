from dotenv import load_dotenv
load_dotenv() # load environment variables

import csv, yaml, os, logging
from datetime import datetime

# --- TODO ---
logging.basicConfig(
    filename = '../../logger.txt',
    level = logging.INFO,
    format = '[%(asctime)s] %(levelname)s | %(message)s',
    encoding = 'utf-8'
)

def process_row(row, config, source):
    if len(row) != len(config['weather_live_basic_data']) + len(config['weather_live_conditions_measurements']):
        return row

    try:
        if source == 'wu':
            row[2] = row[2].replace('EEST', '').strip()

            row_time = datetime.strptime(row[2], "%I:%M %p on %B %d, %Y")
            cleaned = row_time.strftime("%Y-%m-%d %H:%M:%S.%f")

            row[2] = cleaned
        else:
            if row[2] is not None:
                dt = datetime.strptime(row[2], "%d/%m/%Y %H:%M") if source == 'meteo' else datetime.strptime(row[2], "%d-%m-%Y %H:%M:%S")
                cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                row[2] = cleaned
    except Exception as e:
        logging.error(f"Error with time converter ({source}): {row[2]} -> {e}")
        
        return row

    row[0] = clean_farm(row[0], config)
    row[6] = clean_temperature(row[6], config)
    row[7] = clean_humidity(row[7], config)
    row[8] = clean_wind_speed(row[8], config)

    if source == 'meteo':
        row[9] = clean_wind_direction(row[9], config)
    
    row[10] = clean_rainfall(row[10], config)
    row[11] = clean_pressure(row[11], config)

    return row

def clean_farm(farm, config):
    if farm is None:
        return None
    
    return farm.split('farm')[1]

def clean_temperature(row, config):
    units = config['results_units']['temperature'].lower()

    if row is None or units is None:
        return None

    row = row.lower().strip()

    if 'f' in row and units == 'c':
        row = row.replace('f', '').strip()
        row = round((float(row) - 32) / 1.8, 1)

    elif 'c' in row and units == 'c':
        row = row.replace('°c', '').strip()

    return row

def clean_humidity(row, config):
    units = config['results_units']['humidity'].lower()
    
    if row is None or units is None:
        return None

    row = row.lower().strip()

    if '%' in row and units == '%':
        row = row.replace('%', '').strip()

    return row

def clean_wind_speed(row, config):
    units = config['results_units']['wind'].lower()
    
    if row is None or units is None:
        return None

    if row is not None:
        row = row.lower().strip()

    if units == 'km/h':
        if 'km/h' in row:
            row = float(row.replace('km/h', '').strip())
        elif 'mph' in row:
            row = float(row.replace('mph', '').strip())
            row = row * 1.60934  # mph → km/h
        elif 'm/s' in row:
            row = float(row.replace('m/s', '').strip())
            row = row * 3.6      # m/s → km/h
        else:
            try:
                row = float(row)
            except Exception:
                return None

    return round(row, 1)

def clean_wind_direction(direction, config):
    units = config['results_units']['direction']
    
    if units is not None:
        return direction
    
    DIRECTION_TO_DEGREES = {
        'N': 0,
        'NNE': 22.5,
        'NE': 45,
        'ENE': 67.5,
        'E': 90,
        'ESE': 112.5,
        'SE': 135,
        'SSE': 157.5,
        'S': 180,
        'SSW': 202.5,
        'SW': 225,
        'WSW': 247.5,
        'W': 270,
        'WNW': 292.5,
        'NW': 315,
        'NNW': 337.5,
    }

    direction = direction.strip().upper()

    if not direction:
        return direction
    
    return DIRECTION_TO_DEGREES.get(direction)

def clean_rainfall(row, config):
    units = config['results_units']['yetos'].lower()
    
    if row is None or units is None:
        return None

    if row is not None:
        row = row.lower().strip()

        if units == 'mm':
            if 'mm' in row:
                row = float(row.replace('mm', '').strip())
            elif 'cm' in row:
                row = float(row.replace('cm', '').strip())
                row = row * 10  # 1 cm = 10 mm
            elif 'inches' in row:
                row = float(row.replace('inches', '').strip())
                row = row * 25.4  # 1 inch = 25.4 mm
            elif 'inch' in row:
                row = float(row.replace('inch', '').strip())
                row = row * 25.4
            elif 'in' in row:
                row = float(row.replace('in', '').strip())
                row = row * 25.4
            else:
                try:
                    row = float(row)
                except Exception:
                    return None

    return round(row, 1)

def clean_pressure(row, config):
    units = config['results_units']['barometer'].lower()
    
    if row is None or units is None:
        return None

    if row is not None:
        row = row.lower().strip()

        if units == 'hpa':
            try:
                if 'hpa' in row:
                    row = float(row.replace('hpa', '').strip())
                elif 'mb' in row:
                    row = float(row.replace('mb', '').strip())
                elif 'mmhg' in row:
                    row = float(row.replace('mmhg', '').strip())
                    row = row * 1.33322
                elif 'inhg' in row:
                    row = float(row.replace('inhg', '').strip())
                    row = row * 33.8639
                elif 'in' in row:
                    row = float(row.replace('in', '').strip())
                    row = row * 33.8639
                else:
                    row = float(row)
            except Exception:
                logging.error(f"ERROR (preprocessing): clean pressure failed.")

                return None

    return round(row, 1)

def open_csv():
    config = load_config()

    for key, value in config['preprocessing'].items():
        staging_path = value['staging']
        cleaned_path = value['cleaned']
        failed_path = value['failed']

        with open(staging_path, encoding = 'utf-8') as staging_file:
            reader = csv.reader(staging_file)
            header = next(reader, None)

            if not header:
                logging.error(f"ERROR (preprocessing): Empty staging file ({key})")
                continue

        if not os.path.exists(failed_path) or os.path.getsize(failed_path) == 0:
            with open(failed_path, 'w', encoding = 'utf-8', newline = '') as failed:
                csv.writer(failed).writerow(header)

        with open(staging_path, encoding = 'utf-8') as staging_file, \
             open(failed_path, 'a', encoding = 'utf-8', newline = '') as failed_file, \
             open(cleaned_path, 'a', encoding = 'utf-8', newline = '') as clean_file:

            reader = csv.reader(staging_file)
            next(reader, None)

            writer_failed = csv.writer(failed_file)
            writer_cleaned = csv.writer(clean_file)

            if clean_file.tell() == 0:
                writer_cleaned.writerow(header)

            for row in reader:
                if not any(cell.strip() for cell in row):
                    continue

                try:
                    cleaned_row = process_row(row, config, key)

                    if 'null' in cleaned_row or 'None' in cleaned_row or '' in cleaned_row:
                        writer_failed.writerow(row)
                        continue

                    ok = False

                    for unit in [
                                    '°C', 'C', '°F', 'F',
                                    '%',
                                    'km/h', 'm/s', 'mph', 'bft', 'bf', 'bfr',
                                    'mm', 'cm', 'in', 'inch', 'inches',
                                    'hpa', 'mb', 'mmHg', 'inHg'
                                ]:
                        
                        if any(unit in str(cell) for cell in cleaned_row):
                            writer_failed.writerow(row)
                            ok = True
                            break
                        
                    if ok is not True:
                        writer_cleaned.writerow(cleaned_row)

                except Exception as e:
                    logging.error(f"{key}: ERROR (preprocessing): for loop checking all rows -> {e}")

                    writer_failed.writerow(row)

                # time.sleep(1.5)

        with open(staging_path, 'w', encoding = 'utf-8', newline = '') as staging:
            csv.writer(staging).writerow(header)

    
def load_config():
    # method for loading the configuration file (config.yaml)
    with open(os.getenv('CONFIG'), 'r') as conf:
        return yaml.safe_load(conf)

open_csv()