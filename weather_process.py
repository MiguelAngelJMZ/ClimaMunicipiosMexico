import argparse
import settings
import logging
import os
import time
import datetime
from pkg_weather.Weather import ClimaMunicipios


logger = settings.getLogger(os.path.basename(__file__)[:-3])

def format_time_log(seconds: int):
    return str(datetime.timedelta(seconds=seconds))

def get_args():

    logger.info("Parsing initialization arguments")
    parser = argparse.ArgumentParser()
    parser.add_argument('-debug_log', type=str, default=settings.default_parameters.get('-debug_log'))
    parser.add_argument('-time_to_wait', type=str, default=86400)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    if not args.debug_log:
        logging.disable(logging.DEBUG)
    time_to_wait = args.time_to_wait
    while True:
        try:
            clima = ClimaMunicipios()
            process = clima.process()
            logger.info(f"Process executed for today, wainting {time_to_wait} until next execution...")
            time.sleep(time_to_wait)

        except Exception as err:
            logger.error(f"Unhandled error: {str(err)}")
            logger.info(f"Waiting {format_time_log(time_to_wait)} until next execution...")
            time.sleep(time_to_wait)


if __name__ == '__main__':
    logger.info("Inizializing app")
    main()