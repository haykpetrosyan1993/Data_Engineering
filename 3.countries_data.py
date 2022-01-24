from datapackage import Package
import pandas as pd

import main_dict

package = Package('https://datahub.io/core/country-list/datapackage.json')

# print list of all resources:
print(package.resource_names)
enum = 101
id = []
country = []
short_name = []
# print processed tabular data (if exists any)
for resource in package.resources:
    if resource.descriptor['datahub']['type'] == 'derived/csv':
        country_list = resource.read()

        for i, j in enumerate(country_list):
            id.append(enum + i)
            country.append(j[0])
            short_name.append(j[1])
# country_table = dict(zip(['id', 'country', 'short_name'], [id, country, short_name]))
# country_table_df = pd.DataFrame(country_table)
# country_table_df.to_excel('country.xlsx')

print(main_dict.stadium_dict[138])
