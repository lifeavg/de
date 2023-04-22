# Alert app

## Option one: spark + kafka

*(all configurations are not for prod usage)*

Better approach for continuos processing.

Will not work just by `docker compose up` because you'll need to start spark jobs.
Also for real use with `cluster` mode need to setup spark not in `standalone` mode at least, use real mail service and prod configurations of all services.

P.S. Just wanted to try spark streaming and get some understanding of kafka

## Option two: use log monitoring tools like `Grafana`
