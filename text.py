def __get_raw_data_dict(self):
    """
    преобразует с помощью генератора записи исходных данных в словарь вида
    {'team':{'resource_id':{'resource_dimension":{'dimension_usage_record_id':value}}}}
    поскольку в БД метка времени представлена
    :return: возвращает распарсенный набор данных в виде словаря с записями по каждому событию
    """
    if self._response is not None:
        records_dict = {}
        for item in self._get_data(self._response, '$'):
            if len(item) < 2:
                break
            team, team_records = item.split('|')
            records_dict[team] = records_dict.get(team, {})
            for record in self._get_data(team_records, ';'):
                record_id, resource_id, resource_dimension, usage_value = \
                    self._parse_record(record)
                records_dict[team][resource_id] = records_dict[team].get(resource_id, {})
                records_dict[team][resource_id][resource_dimension] = \
                    records_dict[team][resource_id].get(resource_dimension, {})
                records_dict[team][resource_id][resource_dimension][record_id] = \
                    records_dict[team][resource_id][resource_dimension].get(record_id, usage_value)
        print('Данные собраны для команд:', records_dict.keys()) if self._debug else None
        return records_dict
    else:
        return None
