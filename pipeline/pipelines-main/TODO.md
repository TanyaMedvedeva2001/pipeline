## Goals

- Base knowledge: Python + Data (DB, Pandas, Web)
- Understand role of data pipelines
- CI/CD (GitHub Actions, Docker)
- Open Source Project


## Plan & points

1. Data piplines intro+frameworks (4 points)
    - prefect (2)
    - dagster (2)

2. Python ecosystem + basic pipelines implementation (11 points)
    - poetry (2!)
    - click (2!)
    - base tasks (5!)
    - pytest (2)

3. Database integration (9 points)
    - DB-connector (2!)
    - SQL tasks (5!)
    - DB-function (`domain_of_url`) (2)

4. CI/CD (5 points)
    - Docker (3)
    - GitHub Actions (2)


## Examination:

    - Max: 29
    - Auto: 25
    - Min: 18


## Recommendations

- Use Click as a CLI framework — https://click.palletsprojects.com/en/8.1.x/setuptools/
- Use `entry_points` to make `pipelines` available in as global command line utility.
- `domain_of_url` can be temporarily replaced with a builtin SQL function like `upper(str)` (`upper('http://google.com') -> `HTTP://GOOGLE.COM')
- poetry run pip install -e .
- configuration in .env file


ToDo (Jan 27):
- [ ] Meet Dagster & Prefect
- [ ] Implement test project with both Dagster & Prefect
- [ ] Create and share GitHub repo with the results
- [ ] Begin to implement "Pipelines"


ToDo (Feb 3):
- [ ] finish Dagster/Prefect (1):
    - [ ] add poetry support (Dagster/Prefect)
    - [ ] add README.txt
    - [ ] add CSV files
- [ ] Pipelines:
    - [ ] fork this repo
    - [x] add poetry support (deps: click)
    - [x] > pipeline list
    - [x] > pipeline run (fake)
    - [x] > poetry run pipeline run  # should work!
    - [ ] configuration in .env file
 
ToDo (March 3):
- [ ] Make tasks real
    - [ ] Integrate with DB (postgresql)
- [ ] Dockerize: `docker run pipelines --mount=.`
- [ ] Basic Tests
- [ ] Run in GitHub

