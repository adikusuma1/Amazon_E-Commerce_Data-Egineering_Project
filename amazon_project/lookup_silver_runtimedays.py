dbutils.widgets.text("weekday","7")

days = int(dbutils.widgets.get("weekday"))
print(days)

dbutils.jobs.taskValues.set(key='weekoutput', value=days)