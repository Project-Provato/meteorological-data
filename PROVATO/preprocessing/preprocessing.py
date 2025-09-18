from dotenv import load_dotenv
load_dotenv() # load environment variables

import csv, yaml, os, logging, re, json
from datetime import datetime, timezone

from metpy.calc import heat_index as implement_heat_index, windchill as implement_windchill
from metpy.units import units

import numpy as np

from zoneinfo import ZoneInfo

def process_row(row, source, config):
    if check_row_length(row, config) is True:
        # logging.error(f"Line 17: Error with row length")/
        return row, {'error': 'check row length'}

    row[0], farm_status = clean_farm(row[0], config)
    row[1], source_status = clean_source(row[1])
    row[2], timedata__status = clean_timedata(row[2], source)
    row[3], crawled_status = clean_crawled(row[3])
    row[4], city_status = clean_city(row[4])
    row[5], nomos_status = clean_nomos(row[5])

    row[6] = clean_temperature(row[6], config)
    row[7] = clean_humidity(row[7], config)
    row[8] = clean_wind_speed(row[8], config)
    row[9] = clean_wind_direction(row[9], row[8]['wind'])
    row[10] = clean_yetos(row[10], config)
    row[11] = clean_barometer(row[11], config)
    row[12] = clean_dew_point(row[12], config)
    row[13] = clean_heat_index(row[13], row[6]['temperature'], row[7]['humidity'], config)
    row[14] = clean_wind_chill(row[14], row[6]['temperature'], row[8]['wind'], config)
    row[15] = clean_solar_radiation(row[15], config)
    
    if farm_status is False or source_status is False or timedata__status is False or crawled_status is False or city_status is False or nomos_status is False:
        return row, {'error': 'basic data'}

    if check_cleaned_row(row) is False:
        # logging.error(f"Line 31: Error with cleaning checker")
        return row, {'error': 'measurements'}

    return row, {'success': 'ok'}

def check_row_length(row, config):
    return True if len(row) != (len(config['weather_live_basic_data']) + len(config['weather_live_conditions_measurements'])) else False

def clean_farm(farm, config):
    try:
        if not 'farm' in farm:
            return {'farm': farm}, False

        cleaned_farm = farm.split('farm')

        if cleaned_farm is None or not len(cleaned_farm) == 2 or not 0 <= int(cleaned_farm[1]) <= len(config['farms']):
            return {'farm': farm}, False

        return {'farm': cleaned_farm[1]}, True
    except Exception as e:
        print(e)
        return {'farm': farm}, False

def clean_source(source):
    if source is None or not source:
        return {'source': source}, False
    
    return {'source': source}, True

def clean_timedata(timedata, source):
    try:
        if timedata is None or source is None:
            return {'timedata': timedata}, False

        cleaned = None

        athens = ZoneInfo("Europe/Athens")

        if source == 'open-meteo':
            dt = datetime.fromtimestamp(int(timedata), tz=ZoneInfo("UTC"))
            cleaned = dt.astimezone(athens).strftime("%Y-%m-%d %H:%M:%S.%f")
            return {"timedata": cleaned}, True

        elif source == 'wu':
            timedata = timedata.replace("EEST", "").strip()
            dt = datetime.strptime(timedata, "%I:%M %p on %B %d, %Y")
            cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        elif source == 'meteo':
            dt = datetime.strptime(timedata, "%d/%m/%Y %H:%M")
            cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        elif source == 'soda':
            dt = datetime.strptime(timedata, "%Y-%m-%d %H:%M:%S")
            cleaned = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        
        if cleaned is None:
            return {'timedata': timedata}, False

        return {'timedata': cleaned}, True
    except Exception as e:
        # logging.error(f"Error with time converter ({source}): {timedata} -> {e}")
        return {'timedata': timedata}, False

def clean_crawled(crawled):
    if crawled is None or not crawled:
        return {'crawled': crawled}, False

    return {'crawled': crawled}, True

def clean_city(city):
    if city is None or not city:
        return {'city': city}, False

    return {'city': city}, True

def clean_nomos(nomos):
    if nomos is None or not nomos:
        return {'nomos': nomos}, False

    return {'nomos': nomos}, True

def clean_temperature(temperature, config):
    try:
        units = get_units('temperature', config)

        if temperature is None or units is None:
            return {'temperature': temperature}

        temperature, units = set_lower_and_strip(temperature, units)

        if 'f' in temperature and units == 'c':
            temperature = temperature.replace('f', '').strip()

            if check_value(temperature) is True:
                temperature = round((float(temperature) - 32) / 1.8, 1)
        elif 'c' in temperature and units == 'c':
            temperature = temperature.replace('°c', '').strip()

        return {'temperature': float(temperature)}
    except Exception as e:
        # logging.error(f"Temperature cleaning failed: 'temperature': {temperature} | 'units': {units} -> {e}")
        print('aaa', e)
        return {'temperature': temperature}

def clean_humidity(humidity, config):
    try:
        units = get_units('humidity', config)

        if humidity is None or units is None:
            return {'humidity': humidity}

        humidity, units = set_lower_and_strip(humidity, units)

        if '%' in humidity and units == '%':
            humidity = humidity.replace('%', '').strip()

        return {'humidity': float(humidity)}
    except Exception as e:
        # logging.error(f"Humidity cleaning failed: 'humidity': {temperature} | 'units': {units} -> {e}")
        print('2', e)
        return {'humidity': humidity}

def clean_wind_speed(wind, config):
    try:
        units = get_units('wind', config)
        
        if wind is None or units is None:
            return {'wind': wind}

        wind_new, units = set_lower_and_strip(wind, units)

        if units == 'km/h':
            if 'km/h' in wind_new:
                wind_new = wind_new.replace('km/h', '').strip()
            elif 'mph' in wind_new:
                wind_new = wind_new.replace('mph', '').strip()

                if check_value(wind_new) is True:
                    wind_new = float(wind_new) * 1.60934  # mph → km/h
            elif 'm/s' in wind_new:
                wind_new = wind_new.replace('m/s', '').strip()

                if check_value(wind_new) is True:
                    wind_new = float(wind_new) * 3.6      # m/s → km/h

        if check_value(wind_new) is False:
            return {'wind': wind}

        return {'wind': round(float(wind_new), 1)}
    except Exception as e:
        print('3', e)
        return {'wind': wind}

def clean_wind_direction(direction, wind_speed):
    try:
        if direction is None:
            return {'direction': direction}

        if '°' in direction:
            direction = direction.replace('°', '').strip()
            return {'direction': float(direction)}
        
        if check_value(direction) is True:
            return {'direction': float(direction)}
        
        if check_value(direction) is False:
            if int(wind_speed) == 0.0:
                return {'direction': -1}

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
            return {'direction': direction}

        return {'direction': float(match)}
    except Exception as e:
        print('4', e)
        return {'direction': direction}

def clean_yetos(yetos, config):
    try:
        units = get_units('yetos', config)
        
        if yetos is None or units is None:
            return {'yetos': yetos}

        yetos_new, units = set_lower_and_strip(yetos, units)

        if units == 'mm':
            if 'mm' in yetos_new:
                yetos_new = yetos_new.replace('mm', '').strip()
            elif 'cm' in yetos_new:
                yetos_new = yetos_new.replace('cm', '').strip()
                
                if check_value(yetos_new) is True:
                    yetos_new = float(yetos_new) * 10  # 1 cm = 10 mm
            elif 'inches' in yetos_new:
                yetos_new = yetos_new.replace('inches', '').strip()
                
                if check_value(yetos_new) is True:
                    yetos_new = float(yetos_new) * 25.4  # 1 inch = 25.4 mm
            elif 'inch' in yetos_new:
                yetos_new = yetos_new.replace('inch', '').strip()
                
                if check_value(yetos_new) is True:
                    yetos_new = float(yetos_new) * 25.4
            elif 'in' in yetos_new:
                yetos_new = yetos_new.replace('in', '').strip()
                
                if check_value(yetos_new) is True:
                    yetos_new = float(yetos_new) * 25.4

            return {'yetos': 0.0}

        if check_value(yetos_new) is False:
            return {'yetos': yetos}

        return {'yetos': round(float(yetos_new), 1)}
    except Exception as e:
        print('5', e)
        return {'yetos': yetos}

def clean_barometer(barometer, config):
    try:
        units = get_units('barometer', config)
        
        if barometer is None or units is None:
            return {'barometer': barometer}

        barometer_new, units = set_lower_and_strip(barometer, units)

        if units == 'hpa':
            if 'hpa' in barometer_new:
                barometer_new = barometer_new.replace('hpa', '').strip()
            elif 'mb' in barometer_new:
                barometer_new = barometer_new.replace('mb', '').strip()
            elif 'mmhg' in barometer_new:
                barometer_new = barometer_new.replace('mmhg', '').strip()
                
                if check_value(barometer_new) is True:
                    barometer_new = float(barometer_new) * 1.33322
            elif 'inhg' in barometer_new:
                barometer_new = barometer_new.replace('inhg', '').strip()

                if check_value(barometer_new) is True:
                    barometer_new = float(barometer_new) * 33.8639
            elif 'in' in barometer_new:
                barometer_new = barometer_new.replace('in', '').strip()

                if check_value(barometer_new) is True:
                    barometer_new = float(barometer_new) * 33.8639

        if check_value(barometer_new) is False:
            return {'barometer': barometer}

        return {'barometer': round(float(barometer_new), 1)}
    except Exception as e:
        # logging.error(f"ERROR (preprocessing): clean barometer failed.")
        print('6', e)
        return {'barometer': barometer}

def clean_dew_point(dew_point, config):
    try:
        if dew_point is None or not dew_point:
            return {'dew_point': None}

        units = get_units('dew_point', config)
        dew_point_new, units = set_lower_and_strip(dew_point, units)

        if 'f' in dew_point_new and units == 'c':
            dew_point_new = dew_point_new.replace('f', '').strip()

            if check_value(dew_point_new) is True:
                dew_point_new = round((float(dew_point_new) - 32) / 1.8, 1)
        elif 'c' in dew_point_new and units == 'c':
            dew_point_new = dew_point_new.replace('°c', '').strip()

        if check_value(dew_point_new) is False:
            return {'dew_point': 0.0}

        return {'dew_point': float(dew_point_new)}
    except Exception as e:
        # logging.error(f"ERROR (preprocessing): clean barometer failed.")
        print('7', e)
        return {'dew_point': dew_point}

def clean_heat_index(heat, temperature, humidity, config):
    try:
        if heat is None or not heat:
            return {'heat_index': None}
        
        units_local = get_units('heat_index', config)
        heat, units_local = set_lower_and_strip(heat, units_local)

        if 'c' in heat and units_local == 'c':
            heat = heat.replace('°c', '').strip()

            if check_value(temperature) is False or check_value(humidity) is False:
                return {'heat_index': heat}

            temp = float(temperature) * units.degC
            hum = float(humidity) * units.percent

            new = implement_heat_index(temp, hum)
            calc = new.to('degC').magnitude.squeeze()

            if np.ma.is_masked(calc) is False:
                return {'heat_index': calc}

        return {'heat_index': None}
    except Exception as e:
        # logging.error(f"ERROR (preprocessing):.")
        print('8', e)
        return {'heat': heat}
    
def clean_wind_chill(wind_chill, temperature, wind_speed, config):
    try:
        if wind_chill is None or not wind_chill:
            return {'wind_chill': None}
        
        units_local = get_units('wind_chill', config)
        wind_chill, units_local = set_lower_and_strip(wind_chill, units_local)

        if 'c' in wind_chill and units_local == 'c':
            wind_chill = wind_chill.replace('°c', '').strip()

            if check_value(wind_chill) is False or check_value(temperature) is False or check_value(wind_speed) is False:
                return {'wind_chill': wind_chill}

            temp = float(temperature) * units.degC
            wind_sp = float(wind_speed) * units.kph

            new = implement_windchill(temp, wind_sp)
            calc = new.to('degC').m

            if np.ma.is_masked(calc) is False:
                return {'wind_chill': calc}

        return {'wind_chill': None}
    except Exception as e:
        # logging.error(f"ERROR (preprocessing):.")
        print('9', e)
        return {'wind_chill': wind_chill}

def clean_solar_radiation(solar_radiation, config):
    try:
        if solar_radiation is None or not solar_radiation:
            return {'solar_radiation': None}

        units = get_units('solar_radiation', config)
        solar_radiation_new, units = set_lower_and_strip(solar_radiation, units)

        if 'w/m²' in solar_radiation_new and 'w/m²' in units:
            solar_radiation_new = solar_radiation_new.replace('w/m²', '').strip()
        elif 'w/m^2' in solar_radiation_new and 'w/m^2' in units:
            solar_radiation_new = solar_radiation_new.replace('w/m^2', '').strip()

        return {'solar_radiation': solar_radiation_new}
    except Exception as e:
        # logging.error(f"ERROR (preprocessing):.")
        print('10', e)
        return {'solar_radiation': solar_radiation}

def check_header(header, config):
    try:
        items = []

        for item in config['weather_live_basic_data']:
            items.append(item)

        for item in config['weather_live_conditions_measurements']:
            items.append(item)

        if header is None or not header == items:
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
            raw_path = value['raw']
            staging_path = value['staging']
            cleaned_path = value['cleaned']
            failed_path = value['failed']

            with open(staging_path, 'r', encoding = 'utf-8', newline = '') as staging_file:
                reader = csv.reader(staging_file)

                header = check_header(next(reader, None), config)

                # if header is None or (len(header) != (len(config['weather_live_basic_data'] + config['weather_live_conditions_measurements']))):
                    # logging.error(f"ERROR (preprocessing): Empty staging file ({key})")
                #     continue

                now = datetime.now(ZoneInfo("Europe/Athens"))

                check_cleaned, cleaned_path = generate_path(cleaned_path, now, 1)
                check_failed, failed_path = generate_path(failed_path, now, 1)
                check_raw, raw_path = generate_path(raw_path, now, 1)

                with open(failed_path, 'a', encoding = 'utf-8', newline = '') as failed_file, \
                    open(cleaned_path, 'a', encoding = 'utf-8', newline = '') as cleaned_file, \
                    open(raw_path, 'a', encoding = 'utf-8', newline = '') as raw_file:

                    if check_cleaned is True:
                        csv.writer(cleaned_file).writerow(check_header(None, config))

                    if check_failed is True:
                        csv.writer(failed_file).writerow(check_header(None, config))
                            
                    if check_raw is True:
                        csv.writer(raw_file).writerow(check_header(None, config))

                    for row in reader:
                        csv.writer(raw_file).writerow(row)

                        cleaned_row, status = process_row(row, key, config)

                        if next(iter(status)) == 'error':
                            csv.writer(failed_file).writerow([list(item.values())[0] for item in row])
                        elif next(iter(status)) == 'success':
                            csv.writer(cleaned_file).writerow([list(item.values())[0] for item in cleaned_row])

            with open(staging_path, 'w', encoding = 'utf-8', newline = '') as staging:
                csv.writer(staging).writerow(header)

    except Exception as e:
        print(e)

def check_value(value):
    if isinstance(value, (int, float)):
        return True

    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

def check_cleaned_row(cleaned_row):
    # if 'null' in v or None in v or '' in v:
    #     return False

    for row in cleaned_row:
        for k, v in row.items():
            if k == 'temperature' or k == 'humidity' or k == 'wind' or k == 'direction' or k == 'yetos' or k == 'barometer':
                if check_value(v) is False:
                    return False
            elif k == 'heat_index' or k == 'wind_chill' or k == 'solar_radiation' or k == 'dew_point':
                if check_value(v) is False and v is not None:
                    return False

    # for unit in [
    #                 '°C', 'C', '°F', 'F',
    #                 '%',
    #                 'km/h', 'm/s', 'mph', 'bft', 'bf', 'bfr',
    #                 'mm', 'cm', 'in', 'inch', 'inches',
    #                 'hpa', 'mb', 'mmHg', 'inHg',
    #                 'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'
    #             ]:

    #     for cell in cleaned_row:
    #         for value in cell.values():
    #             if isinstance(value, str) and unit in value:
    #                 print('aaa', value, unit, cell)
    #                 return False

    return True

init_preprocessing()
