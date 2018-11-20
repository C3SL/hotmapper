''' This module contains functions to generate pairing reports '''
import os
import pandas as pd

from mapping_functions.table_manipulation import walk_tables
import settings


def generate_pairing_csv(engine):
    '''Generates pairing reports in csv format. Generates a file for each table'''
    for table_name, pairing, _ in walk_tables(engine):
        output_file = table_name + '.csv'
        output_file = os.path.join(settings.PAIRING_OUTPUT_FOLDER, output_file)
        pairing.to_csv(output_file, index=False)


def generate_pairing_xlsx(engine):
    '''Generate pairing reports in xlsx format on a single file, where each sheet corresponds
    to one of the tables'''
    xls_output_name = os.path.join(settings.PAIRING_OUTPUT_FOLDER,
                                   settings.XLS_OUTPUT_FILE_NAME)
    xls_writer = pd.ExcelWriter(xls_output_name, engine='xlsxwriter')

    workbook = xls_writer.book

    merge_format = workbook.add_format({
        'align': 'center',
        'valign': 'vcenter'})

    for table_name, pairing, variable_sizes in walk_tables(engine):
        pairing.to_excel(xls_writer, sheet_name=table_name, index=False)
        worksheet = xls_writer.sheets[table_name]
        current_line = 1
        for size in variable_sizes:
            line_range = [current_line + 1, current_line + size]
            merge_ranges = [chr(65+i) + '{}:' + chr(65+i) + '{}' for i in range(4)]
            merge_ranges = [m.format(*line_range) for m in merge_ranges]
            for i, merge_range in enumerate(merge_ranges):
                worksheet.merge_range(merge_range, pairing.iloc[current_line-1, i], merge_format)

            current_line += size

    xls_writer.save()
