import awswrangler as wr
from boto3 import Session
import os
import sys
import pandas as pd
import unicodedata
from datetime import datetime
from src.utils.logger import logger_config
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

try:
    from awsglue.utils import getResolvedOptions 
except ImportError:
    print("run locally...")

logger = logger_config()

class Normalize():
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df
    
    def str_column(self, column: str) -> pd.Series:    
        self.df[column] = self.df[column].apply(lambda x: str(x).strip() if str(x).lower() != 'nan' else x)
        self.df[column] = self.df[column].apply(lambda x: ''.join(ch for ch in unicodedata.normalize('NFKD', x) if not unicodedata.combining(ch)) if str(x).lower() != 'nan' and type(x) is not float else x)
        self.df[column] = self.df[column].apply(lambda x: str(x).lower() if str(x).lower() != 'nan' else x)
        return self.df[column].astype("string")
    
    def float_column(self, column: str) -> pd.Series:
        self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        self.df.loc[self.df[column].isna() , column] = 0.0
        return self.df[column].astype('Float64')
    
    def int_column(self, column: str) -> pd.Series:
        self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        self.df.loc[self.df[column].isna() , column] = 0
        return self.df[column].astype('Int64')

def main(event):
    region_name = event['region_name']
    bucket_name = event['bucket_name']
    path_suffix = event['path_suffix']

    try:
        session = Session(region_name=region_name)
        df_travel = wr.s3.read_csv(path=f's3://{bucket_name}/', path_suffix=path_suffix, boto3_session=session, encoding = "ISO-8859-1", sep=';')
        
        if df_travel.empty:
            logger.info('There is no new data in the bucket')
            exit()

        df_travel.drop(columns=['Função', 'Descrição Função'], inplace=True)

        df_travel.rename(columns={
            'Identificador do processo de viagem': 'trip_process_id', 
            'Número da Proposta (PCDP)': 'proposal_number_PCDP',
            'Situação': 'trip_realized', 
            'Viagem Urgente': 'urgent_travel', 
            'Justificativa Urgência Viagem': 'justification_travel_urgency',
            'Código do órgão superior': 'higher_body_code', 
            'Nome do órgão superior': 'higher_body_name',
            'Código órgão solicitante': 'requesting_body_code', 
            'Nome órgão solicitante': 'requesting_body_name', 
            'CPF viajante': 'traveler_tax_id',
            'Nome': 'name', 
            'Cargo': 'position',
            'Período - Data de início': 'start_date', 
            'Período - Data de fim': 'end_date', 
            'Destinos': 'destinations',
            'Motivo': 'reason', 
            'Valor diárias': 'daily_value', 
            'Valor passagens': 'tickets_value', 
            'Valor devolução': 'return_value',
            'Valor outros gastos': 'other_expenses_value'
        }, inplace=True)

        df_travel = df_travel[df_travel['trip_process_id'].notna()]

        logger.info('Start normalize data!')
        normalize = Normalize(df=df_travel)
        
        df_travel['trip_realized'] = normalize.str_column(column='trip_realized')
        df_travel['urgent_travel'] = normalize.str_column(column='urgent_travel')
        df_travel['justification_travel_urgency'] = normalize.str_column(column='justification_travel_urgency')
        df_travel['higher_body_name'] = normalize.str_column(column='higher_body_name')
        df_travel['requesting_body_name'] = normalize.str_column(column='requesting_body_name')
        df_travel['name'] = normalize.str_column(column='name')
        df_travel['position'] = normalize.str_column(column='position')
        df_travel['destinations'] = normalize.str_column(column='destinations')
        df_travel['reason'] = normalize.str_column(column='reason')
        df_travel['proposal_number_PCDP'] = normalize.str_column(column='proposal_number_PCDP')

        df_travel['higher_body_code'] = normalize.int_column(column='higher_body_code')
        df_travel['requesting_body_code'] = normalize.int_column(column='requesting_body_code')

        df_travel['daily_value'] = normalize.float_column(column='daily_value')
        df_travel['tickets_value'] = normalize.float_column(column='tickets_value')
        df_travel['return_value'] = normalize.float_column(column='return_value')
        df_travel['other_expenses_value'] = normalize.float_column(column='other_expenses_value')

        df_travel['start_date'] = df_travel['start_date'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").date().isoformat() if str(x).lower() not in ['nan', 'nat'] else pd.NaT)

        df_travel['end_date'] = df_travel['end_date'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").date().isoformat() if str(x).lower() not in ['nan', 'nat'] else pd.NaT)

        df_travel['trip_realized'] = df_travel['trip_realized'].astype(object)
        df_travel.replace({'trip_realized': {'realizada': True, 'nao realizada': False}}, inplace=True)

        df_travel['urgent_travel'] = df_travel['urgent_travel'].astype(object)
        df_travel.replace({'urgent_travel': {'sim': True, 'nao': False}}, inplace=True)

        date = datetime.now().date()

        df_travel['year'] = date.year
        df_travel['month'] = date.month
        df_travel['day'] = date.day

        logger.info('Completion of data normalization!')

        logger.info('Start saving the normalized data in the bucket!')

        wr.s3.to_parquet(
            df=df_travel,
            path=f"s3://{bucket_name}/stage_area/travel/",
            dataset=True,
            partition_cols=['year', 'month', 'day'],
            mode='append',
            schema_evolution=True
        )

        logger.info('The normalized data was saved in the bucket!')

    except Exception as e:
        logger.exception(e)
        raise e

if __name__ == "__main__":
    try:
        event = getResolvedOptions(sys.argv, 
        [
            'region_name',
            'bucket_name',
            'path_suffix'
        ])
    except NameError:
        event = {
            'region_name': os.getenv('REGION_NAME'),
            'bucket_name': os.getenv('BUCKET_NAME'),
            'path_suffix': os.getenv('PATH_SUFFIX')
        }

    logger.info(f'Event: {event}')
    main(event)



