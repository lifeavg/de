# Students dormitory scripts

To run script from `Task_1_Python_introduction` folder use:

```bash
python main.py -h
```

`init` command shout be used once for DB. Will create tables structure

Init script assumes that DB already exists

To run scripts on DB use update configuration in `config.json` template

Before creating reports `extract` data from files to DB. Use `python main.py extract -h` for more information.

Use `report` command to create a report. `python main.py report -h` for more information.
