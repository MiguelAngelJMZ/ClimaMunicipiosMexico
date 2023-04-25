import polars as pl
from polars import DataFrame
import random
import json
import datetime
import glob
import os
import io
import settings

logger = settings.getLogger(os.path.basename(__file__)[:-3])

class ClimaMunicipios:

    def __int__(self):
        self.format = "%Y-%m-%d-%H"

    def current_str_datetime(self, format:str) -> str:
        try:
            logger.info("Getting current datetime")
            current = datetime.datetime.now().strftime(format)
            return current
        except Exception as err:
            logger.error(f"An error has ocurred: {err}")
            raise err


    def max_folder_file(self, folder:str) -> str:
        try:
            list_of_files = glob.glob(folder)
            latest_file = max(list_of_files, key=os.path.getctime)
            return latest_file
        except Exception as err:
            logger.error(f"An error has ocurred: {err}")
            raise err

    def generate_json_response(self, filename_response:str):
        try:
            logger.info("Calculating the municipalities by State")
            folder_entry = self.max_folder_file("data_municipios/*")
            df = pl.read_csv(f"{folder_entry}/data.csv")
            df_num_mun = df.groupby("Cve_Ent").agg(pl.struct("Cve_Mun").n_unique().alias('num_mun'))
            dict_est_mun = {}
            for a, b in zip(df_num_mun["Cve_Ent"], df_num_mun["num_mun"]):
                dict_est_mun[a] = b
            list_mun_clima = []

            logger.info("Building dictionaries with relevant information about whether per municipality")
            for key, value in dict_est_mun.items():
                for mun in range(0, value):
                    clima_dict = {}
                    clima_dict["cc"] = random.uniform(89, 91)
                    clima_dict["ides"] = key
                    clima_dict["idmun"] = mun
                    clima_dict["dirvieng"] = random.randint(4, 10)
                    clima_dict["prec"] = random.uniform(0.50, 1.50)
                    clima_dict["probprec"] = random.uniform(0, 100)
                    clima_dict["raf"] = random.uniform(14, 30)
                    clima_dict["tmax"] = random.uniform(15, 40)
                    clima_dict["tmin"] = random.uniform(0, 15)
                    clima_dict["velvien"] = random.uniform(0, 100)
                    clima_dict["probprec"] = random.uniform(0, 15)

                    list_mun_clima.append(clima_dict)

            mun_clima_json = json.dumps(list_mun_clima)

            logger.info("Saving generated JSON file")
            with open(f"json_clima_municipios/{filename_response}.json", 'w') as outfile:
                outfile.write(mun_clima_json)

            return mun_clima_json

        except Exception as err:
            logger.error(f"An error has ocurred: {err} ")
            raise err

    def save_json_to_csv(self, curr_mun_clima_json, curr_filename:str) -> pl.DataFrame:
        try:
            logger.info("Saving current JSON file as a CSV file")
            mun_clima_io = io.StringIO(curr_mun_clima_json)
            df_mun_clima = pl.read_json(mun_clima_io)
            df_mun_clima.write_csv(f"csv_clima_municipios/{curr_filename}.csv")
            return  df_mun_clima
        except Exception as err:
            raise err

    def join_and_avg_weather(self, df_clima_curr: DataFrame) -> pl.DataFrame:
        try:
            logger.info("Reading last information about wheater per municipality")
            latest_file = self.max_folder_file("csv_clima_municipios/*")
            df_clima_prev = pl.read_csv(latest_file)

            logger.info("Join both current and last information about wheather per municipality")
            df_clima = df_clima_prev.join(df_clima_curr, on=["ides", "idmun"])
            left_cols = ["dirvieng", "prec", "probprec", "raf", "tmax", "tmin", "velvien"]
            right_cols = ["dirvieng_right", "prec_right", "probprec_right", "raf_right", "tmax_right", "tmin_right",
                          "velvien_right"]
            new_cols = ["ides", "idmun"]

            logger.info("Calculate avg on relevant columns, also remaining columns")
            for l, r in zip(left_cols, right_cols):
                name = f"{l}_t"
                new_cols.append(name)
                df_clima = df_clima.with_columns(((pl.col(l) + pl.col(r))/2).alias(name))
            df_clima_avg = df_clima[new_cols]
            df_clima_avg.columns = list(map(lambda x: x.replace("_t", ""), df_clima_avg.columns))
            return df_clima_avg

        except Exception as err:
            logger.error(f"An error has ocurred: {err}")
            raise err


    def join_weather_municipality_data(self,  df_clima_avg: DataFrame,
                                        filename_response: str):

        logger.info("Reading last file about weather avg per municipality")
        latest_file = self.max_folder_file("data_municipios/*")
        df_mun = pl.read_csv(f"{latest_file}/data.csv")

        logger.info("Joining both current and last information about weather avg per municipality")
        df_clima_mun_avg = df_mun.join(df_clima_avg, left_on=['Cve_Ent', 'Cve_Mun'], right_on=['ides', 'idmun'],
                                       how='left')

        logger.info("Saving information about weather abg municipality")
        df_clima_mun_avg.write_csv(f"clima_municipios_avg/{filename_response}.csv")
        df_clima_mun_avg.write_csv(f"clima_municipios_avg/current.csv")


    def process(self):
        current_datetime = self.current_str_datetime("%Y-%m-%d-%H")
        curr_mun_clima_json = self.generate_json_response(current_datetime)
        df_clima_curr = self.save_json_to_csv(curr_mun_clima_json=curr_mun_clima_json, curr_filename=current_datetime)
        df_clima_avg = self.join_and_avg_weather(df_clima_curr=df_clima_curr)
        self.join_weather_municipality_data(df_clima_avg=df_clima_avg, filename_response=current_datetime)
        return True






