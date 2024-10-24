# script per modificare il file calendar_dates.txt

path_to_GTFS = 'GTFS_230406_240405/'
input_file = path_to_GTFS + 'calendar_dates.txt'
trip_file = path_to_GTFS + 'trips.txt'
output_file = path_to_GTFS + 'new_calendar_dates.txt'
header = 'service_id,day,exception_type'

with open(trip_file, 'r') as trips:
    for line_num, line in enumerate(trips):
        if line_num == 0:
            # Istanzia la lista per memorizzare i service_id
            service_id = []
        else:
            # Aggiunge un nuovo service_id nella lista
            parts = line.strip().split(',')
            if parts[1] not in service_id:
                service_id.append(parts[1])

print(f'I service_id utilizzati nel file trips sono: {len(service_id)}')
print(f'E sono i seguenti: {service_id}')

print('Modifico il file Calendar_dates.txt in un nuovo file new_calendar_dates.txt valutando solo i trips utilizzati...')
with open(input_file, 'r') as file_in, open(output_file, 'w') as file_out:
    for line_num, line in enumerate(file_in):
        if line_num == 0:
            # Scrive la prima riga senza modifiche nel file di output
            file_out.write(header + '\n')
        else:
            # Modifica le righe successive differenziando l'anno il mese e il giorno
            parts = line.strip().split(',')
            if parts[0] in service_id:
                date = parts[1]
                modified_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
                modified_line = f"{parts[0]},{modified_date},{parts[2]}"
                file_out.write(modified_line + '\n')
