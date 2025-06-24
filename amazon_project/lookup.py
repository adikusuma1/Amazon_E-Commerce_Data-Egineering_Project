files = [
    {
        "source_folder":"Cloud Warehouse Compersion Chart",
        "target_folder":"Cloud Warehouse Compersion Chart",
    },
    {
        "source_folder":"Expense IIGF",
        "target_folder":"Expense IIGF",
    },
    {
        "source_folder":"International sale Report",
        "target_folder":"International sale Report",
    },
    {
        "source_folder":"May-2022",
        "target_folder":"May-2022",
    },
    {
        "source_folder":"P  L March 2021",
        "target_folder":"P  L March 2021",
    },
    {
        "source_folder":"Sale Report",
        "target_folder":"Sale Report",
    }
]

dbutils.jobs.taskValues.set(key = "data_array", value = files)