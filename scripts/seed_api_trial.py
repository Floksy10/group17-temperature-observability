"""Seed synthetic active histories in final_bench ONLY, no course messages."""
import json
import os
from pathlib import Path
import psycopg


def main():
    dsn = os.environ['DATABASE_URL']
    assert psycopg.conninfo.conninfo_to_dict(dsn)['dbname'] == 'final_bench'
    fixtures = []
    with psycopg.connect(dsn) as connection:
        connection.execute(Path('database/schema.sql').read_text())
        with connection.cursor() as cursor:
            for e in range(10):
                experiment = f'active-long-{e}'
                cursor.execute('INSERT INTO experiments (experiment_id, researcher, sensors, lower_threshold, upper_threshold, started) VALUES (%s,%s,%s,%s,%s,TRUE)', (experiment, 'test@example.com', ['s'], 25.5, 26.5))
                points = []
                with cursor.copy('COPY measurements (experiment_id,measurement_id,measured_at,temperature,out_of_range,during_experiment) FROM STDIN') as copy:
                    for i in range(5000):
                        timestamp = 1700000000. + i
                        temperature = 26. if i % 4 else 28.
                        outside = temperature > 26.5
                        copy.write_row((experiment, f'm{i:05d}', timestamp, temperature, outside, True))
                        points.append(dict(timestamp=timestamp, temperature=temperature, outside=outside))
                fixtures.append(dict(id=experiment, points=points))
    Path('/src/docs/final-evidence/active-fixtures.json').write_text(json.dumps(fixtures))
    print('Seeded 10 active experiments x 5000 points in final_bench')


if __name__ == '__main__':
    main()
