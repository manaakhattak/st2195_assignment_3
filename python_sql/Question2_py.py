import pandas as pd
import sqlite3

# Load data into pandas DataFrames
ontime_2000 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2000_50000_ontime.csv')
ontime_2001 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2001_50000_ontime.csv')
ontime_2002 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2002_50000_ontime.csv')
ontime_2003 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2003_50000_ontime.csv')
ontime_2004 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2004_50000_ontime.csv')
ontime_2005 = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/2005_50000_ontime.csv')
airports = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/airports.csv')
carriers = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/carriers.csv')
planes = pd.read_csv('C:/Users/Manaa.DESKTOP-VK0JS4M/Dropbox/University of London/Programming for Data Science/Practice Assignments/Live Session 3 Practice Assignment/data_sets/plane-data.csv')

# Concatenate ontime data
ontime = pd.concat([ontime_2000, ontime_2001, ontime_2002, ontime_2003, ontime_2004, ontime_2005], ignore_index=True)

# Create SQLite database
conn = sqlite3.connect('airline2.db')

# Save data to SQLite database
ontime.to_sql('ontime', conn, if_exists='replace', index=False)
airports.to_sql('airports', conn, if_exists='replace', index=False)
carriers.to_sql('carriers', conn, if_exists='replace', index=False)
planes.to_sql('planes', conn, if_exists='replace', index=False)

# Query to find the airplane with the lowest average departure delay
query = """
SELECT
    planes.tailnum,
    AVG(ontime.DepDelay) as avg_departure_delay
FROM
    ontime
JOIN
    planes ON ontime.TailNum = planes.tailnum
WHERE
    ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay IS NOT NULL
GROUP BY
    planes.tailnum
ORDER BY
    avg_departure_delay ASC
LIMIT 1;
"""

# Execute the query
result = pd.read_sql(query, conn)

# Close the connection
conn.close()

# Display the result
print(result)