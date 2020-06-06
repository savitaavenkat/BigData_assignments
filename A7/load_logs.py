import datetime
import re,os,gzip
import sys
import uuid
from cassandra.query import BatchStatement
from cassandra.cluster import Cluster
cluster = Cluster(['199.60.17.188', '199.60.17.216'])

def splitline(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] "[A-Z]+ (\S+) HTTP/\d\.\d" \d+ (\d+)$')
    match = re.search(line_re, line)
    if match:
        str_ = re.search(line_re, line)
        hostname = str_.group(1)
        date = datetime.datetime.strptime(str_.group(2), '%d/%b/%Y:%H:%M:%S')
        path = str_.group(3)
        bytes = int(str_.group(4))
        id = str(uuid.uuid1())
        return [hostname, date, path, bytes,id]
    return None

def main(inputs,output,keyspace):
    # main logic starts here
    count  = 1
    add_query = session.prepare("INSERT INTO " + output + "(host, id, datetime, path, bytes) VALUES (?, uuid(), ?, ?, ?)")
    batch = BatchStatement(consistency_level = 1)
    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                list_values = splitline(line)
                if not list_values is None:
                    hostname = list_values[0]
                    date = list_values[1]
                    path = list_values[2]
                    bytes = list_values[3]
                    id = list_values[4]
                    count = count + 1
                    batch.add(add_query, (hostname,date,path,bytes))
                    if count == 300:
                        session.execute(batch)
                        batch.clear()
                        count = 1
            session.execute(batch)
            batch.clear()


if __name__ == '__main__':

    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    output = sys.argv[3]
    session = cluster.connect(keyspace)
    main(inputs, output, keyspace)
