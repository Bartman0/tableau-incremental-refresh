# tableau-incremental-refresh
Project that enables you to perform realistic incremental refreshes in a Tableau Server environment with databases as source system. It uses the Tableau Hyper, Document and Server Client API's to accomplish this. Plus a [modified/forked Tableau document API](https://github.com/Bartman0/document-api-python/tree/extract) by me.

The main goal of this code is to implemenent incremental refreshes within Tableau taking updates of existing data into consideration. The current incremental refresh of Tableau data sources is very limited: it only supports cases where only new data is added to the data(base) underlying a data source, but this is rarely the case in data warehouses. In all data warehouses that I know of, data is also updated frequently to reflect the latest attribute and measure values. And then the standard Tableau incremental refresh mode can not be applied. Only one option remains: a full refresh of data sources.
If your data warehouse is updated daily, and only data is added and updated for the last couple of days, it is very expensive to perform a full refresh of the data source for the whole history which can go up to several years of data. Just because a very limited amount of data is updated instead of merely added to the system.

This component fixes that: it uses the definition of specified data sources to perform several steps:
* it retrieves the data source from the server
* it determines the underlying extract Hyper file
* it removes to-be-updated data from that Hyper file
* it updates the incremental refresh information in the data soruce
* it deploys the updated data source to the server
* it schedules a regular incremental refresh

By cleaning the Hyper extract file from to-be-updated data and updating the incremental refresh information in the data source, the regular incremental refresh as performed by Tableau server can be applied succesfully again.
This way, data source refreshes that could take hours for large data sources, data wise of history wise, can be performed within minutes.

Of course, more documentation is needed to get this functionality implemented in your situation. That will follow later when this component is fully developed and tested more rigorously.
