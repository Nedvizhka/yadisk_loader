def get_saved_files_names(source):
    try:
        with open(f'uploaded_files_{source}.txt', 'r') as uploaded_txt:
            list_loaded_files = uploaded_txt.read()
        uploaded_txt.close()
        list_loaded_files = list_loaded_files.split('\n')[:-1]
    except:
        list_loaded_files = []
    return list_loaded_files

def write_saved_file_names(filename, source):
    with open(f'uploaded_files_{source}.txt', 'a+') as uploaded_txt:
        uploaded_txt.write(f"{filename}\n")
    uploaded_txt.close()

files_avito_current = get_saved_files_names('avito')

print(files_avito_current)

write_saved_file_names('24-03-23 (без дублей)', 'avito')

files_avito_current = get_saved_files_names('avito')

print(files_avito_current)
