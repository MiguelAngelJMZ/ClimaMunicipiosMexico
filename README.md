# ClimaMunicipiosMexico

### Folders
- clima_municipios_avg: folder where the information about the avg of current and previous weather hour are saved. It is saved in CSV format. The filenames have the next structure: "YYYY-MM-DD-HH" and it's also save the current version of AVG Weather.
- csv_clima_municipios: folder where the information about weather hour is saved. It is saved in CSV format. The filenames have the next structure: "YYYY-MM-DD-HH"
- data_municipios: folder where the information about municipality general information is saved. It is saved in a carpet by month.
- json_clima_municipios: folder where the information about weather hour is saved. It is saved in JSON format. The filenames have the next structure: "YYYY-MM-DD-HH"
- pkg_util: it is used to saved code that can be reused in several parts of the code.
- pkg_weather: folder where lives the process to generate JSON Weather and calculate AVG weather
- settings: it is used to make certain configurations for the code.
- weather_prcess.py: it is used to execute the code for all the process of Municipality Weather.


### The pourposes of this repository are the next:

1. Every hour, the Conagua Wen Service must be consumed to get recent data. This Web Service was not working, instead one function was created to simulate JSON response: pkg_weather.Weather.ClimaMunicipios.generate_json_response
2. Generate a table based on data extracted for Point 1 and the previous hour, the avg for relevant variables must be calculated. The next function was created for this step: pkg_weather.Weather.ClimaMunicipios.join_and_avg_weather
3. Generate a table based on data for Point 2 and the last file for data_municipios folder. The next function was created for this step: pkg_weather.Weather.ClimaMunicipios.join_weather_municipality_data

### How to execute

#### Use using the apropriate configuration, using:

- Script Path: ClimaMunicipiosMexico\weather_process.py
- Working Directory: ClimaMunicipiosMexico