import gdown
import requests
import pandas as pd
from pathlib import Path
from epiweeks import Year
from bs4 import BeautifulSoup


class ETL:
    def __init__(self) -> None:
        self.url_datasets = 'https://drive.google.com/drive/folders/12AHywbYCOn9bsf4nDgkMpmlBp5lSw_0q'
        self.url_dengue_dataset = 'https://www.datosabiertos.gob.pe/dataset/vigilancia-epidemiol%C3%B3gica-de-dengue'
        self.data_path = Path('data', 'external')
        self.data_path_str = str(self.data_path)
        self.save_path = Path('data', 'processed')


    def get_g_drive_files(self):
        gdown.download_folder(url=self.url_datasets, output=self.data_path_str, quiet=True, use_cookies=True)


    def datos_abiertos(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0'
        }
        r = requests.get(self.url_dengue_dataset, headers=headers)

        if r.status_code==200:
            soup = BeautifulSoup(r.content, 'html.parser')

            link_elements = soup.find_all('a')

            for link_element in link_elements:
                if 'Descargar' in link_element.text:
                    print(link_element)
                    break

            url_dengue_csv = link_element['href']

            csv_dengue_file_name = url_dengue_csv.split('/')[-1]

            r1 = requests.get(url_dengue_csv, headers=headers)

            with open(self.data_path.joinpath(csv_dengue_file_name), 'w', encoding='utf-8') as csv_dengue_file:
                csv_dengue_file.write(r1.text.replace('\,', '-'))


    def extract(self):
        self.get_g_drive_files()
        self.datos_abiertos()


    def dengue(self):
        df_dengue = pd.read_csv(self.data_path.joinpath('datos_abiertos_vigilancia_dengue.csv'), dtype={'localcod': str})
        df_dengue = df_dengue[(df_dengue['departamento']=='LORETO') & (df_dengue['ano']>=2017) & (df_dengue['ano']<=2022)].copy()
        df_dengue.rename(columns={'departamento': 'department',
                                'provincia': 'province',
                                'distrito': 'district',
                                'enfermedad': 'disease',
                                'ano': 'year',
                                'semana': 'epi_week',
                                'edad': 'age',
                                'tipo_edad': 'age_type',
                                'sexo': 'gender'},
                        inplace=True)

        self.df_dengue = df_dengue.sort_values(['year', 'epi_week'])

        self.df_dengue['age'] = self.df_dengue['age'].astype('float32')
        self.df_dengue.loc[self.df_dengue['age_type']=='M', 'age'] = self.df_dengue.loc[self.df_dengue['age_type']=='M', 'age']/12
        self.df_dengue.loc[self.df_dengue['age_type']=='D', 'age'] = self.df_dengue.loc[self.df_dengue['age_type']=='D', 'age']/365

        groupby_list = ['ubigeo', 'year', 'epi_week']
        df_dengue_agg = self.df_dengue.groupby(groupby_list).agg({'disease': 'count',
                                                            'age': ['mean', 'median'],
                                                            'gender': pd.Series.mode
                                                            }).reset_index()
        df_dengue_agg.columns = ['ubigeo', 'year', 'epi_week', 'n_cases', 'age_mean', 'age_median', 'gender_mode']
        df_dengue_agg.loc[~df_dengue_agg['gender_mode'].isin(['F', 'M']), 'gender_mode'] = 'B'

        return df_dengue_agg


    def districts(self):
        df_districts = pd.read_csv(self.data_path.joinpath('districts_2017census.csv'))
        self.ubigeos_loreto = df_districts[df_districts['departmento']=='LORETO']['ubigeo'].sort_values().to_list()
        df_districts_loreto = df_districts[df_districts['departmento']=='LORETO'].copy()
        df_districts_loreto.drop(columns='source', inplace=True)
        df_districts_loreto.rename(columns={'departmento': 'department',
                                            'provincia': 'province',
                                            'distrito': 'district'},
                                    inplace=True)

        return df_districts_loreto


    def population(self):
        df_population = pd.read_csv(self.data_path.joinpath('population_2017-2022.csv'))
        df_population_loreto = df_population[df_population['ubigeo'].isin(self.ubigeos_loreto)].copy()
        return df_population_loreto


    def temperature(self):
        df_temperature = pd.read_csv(self.data_path.joinpath('mintemp_20170101-20221231.csv'))
        df_temperature_loreto = df_temperature[df_temperature['ubigeo'].isin(self.ubigeos_loreto)].copy()
        col_temp = df_temperature_loreto.columns.to_list()
        df_temperature_loreto_agg = pd.DataFrame()

        for year in self.df_dengue['year'].sort_values().unique():
            for ix, week in enumerate(Year(year).iterweeks(), 1):
                start_week = week.startdate()
                start_week_str = start_week.strftime('%Y%m%d')
                column_name = f'mintemp_{start_week_str}'
                col_ix = col_temp.index(column_name)
                df_temp_agg = df_temperature_loreto.iloc[:, col_ix: col_ix+7].agg(['min', 'mean', 'median', 'max'], axis=1)
                df_temp_agg.loc[:, 'ubigeo'] = df_temperature_loreto.loc[:, 'ubigeo']
                df_temp_agg.loc[:, 'year'] = year
                df_temp_agg.loc[:, 'epi_week'] = ix
                df_temp_agg.loc[:, 'week_start_date'] = start_week
                df_temperature_loreto_agg = pd.concat([df_temperature_loreto_agg, df_temp_agg], ignore_index=True)

        df_temperature_loreto_agg.rename(columns={'min': 'week_min_temp',
                                                'mean': 'week_mean_temp',
                                                'median': 'week_median_temp',
                                                'max': 'week_max_temp'},
                                        inplace=True)

        return df_temperature_loreto_agg


    def transform(self):
        df_dengue_agg = self.dengue()
        df_districts_loreto = self.districts()
        df_population_loreto = self.population()
        df_temperature_loreto_agg = self.temperature()
        df_dengue_loreto_week = pd.merge(df_dengue_agg, df_temperature_loreto_agg,
                                         on=['ubigeo', 'year', 'epi_week'], how='outer')
        df_dengue_loreto_week = pd.merge(df_dengue_loreto_week, df_districts_loreto, on='ubigeo', how='left')
        df_dengue_loreto_week = pd.merge(df_dengue_loreto_week, df_population_loreto, on=['ubigeo', 'year'], how='left')
        self.df_dengue_loreto_week = df_dengue_loreto_week.sort_values(['year', 'epi_week', 'ubigeo'], ignore_index=True)



    def load(self):
        self.df_dengue_loreto_week.to_csv(self.save_path.joinpath('dengue_loreto_SE.csv'), index=False)
