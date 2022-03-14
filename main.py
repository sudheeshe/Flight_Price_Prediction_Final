from Training_raw_data_validation.rawValidation import rawDataValidation
from application_logger.logging import AppLogger



regex = "['data']+['\_'']+[\d]+\.xlsx"
a = rawDataValidation('Training_Batch_Files')
q,w,e = a.values_from_schema()

print(q)

a.validation_file_name_raw(regex, q)

a.validate_missing_values_in_whole_columns()