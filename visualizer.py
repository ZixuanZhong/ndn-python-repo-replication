import sqlite3
import time
 

def execute_sql(db, sql): 
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute(sql)
    conn.commit()
    result = c.fetchall()
    conn.close()
    return result
        

def main():
    while True:
        now = time.time()
        print('----- TIMESTAMP: {0:.0f} -------------'.format(now))
        sql = 'SELECT C.node_name, B.data_name, CASE WHEN C.valid_thru>={0:.0f} THEN "active" ELSE "non-active" END FROM Data_nodes A, data B, nodes C WHERE A.data_id=B.id AND A.node_id=C.id ORDER BY C.id ASC, B.id ASC;'.format(now)
        result = execute_sql('sqlite/catalog.db',sql)

        now = time.time()
        sql2 = 'SELECT node_name, CASE WHEN valid_thru>={0:.0f} THEN "active" ELSE "non-active" END FROM nodes ORDER BY id ASC;'.format(now)
        result2 = execute_sql('sqlite/catalog.db',sql2)


        nodes_dict = {}
        for node in result2:
            node_dict = {}
            node_dict['active'] = node[1]
            node_dict['data'] = []
            nodes_dict[node[0]]=node_dict

        for row in result:
            node_name = row[0]
            node_dict = nodes_dict[node_name]
            node_dict['data'].append(row[1])


        for key, value in nodes_dict.items():
            print('({1})\t[{0}]'.format(key, value['active']))
            if len(value['data']) == 0:
                print('\t\t\t-')
            for data in value['data']:
                print('\t\t\t{}'.format(data))
        print('-----------------------------------------')
        print('\n')

        # print(result)
        time.sleep(3)
        # print(result2)
        # print(nodes_dict)


if __name__ == '__main__':
    main()