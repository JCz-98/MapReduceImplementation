# 2020/09/15
# Desarrollado por: Jonathan Cazco
# MapReduce implementation

import docx
import os
from pathlib import Path
import math
import re
from functools import reduce
import threading
import sys

project_name = ''
dir_name = ''
failsafe_loop = 1
map_failed = False
reduce_nodes_reset = []


def setup_input(file_name):
    global project_name
    global dir_name

    project_name = Path(file_name).stem.lower()
    dir_name = 'data/' + project_name
    line_index = 1
    file_index = 1
    create_data_dir(dir_name)
    files_base_name = dir_name + '/input{}.txt'.format(file_index)

    if file_name.lower().endswith('docx'):
        wdoc = docx.Document(file_name)
        f = open(files_base_name, 'w')

        for paragraph in wdoc.paragraphs:
            line = paragraph.text.strip()
            if line:
                if line_index <= 25:
                    f.write(line + '\n')
                    line_index = line_index + 1
                else:
                    f.close()
                    file_index = file_index + 1
                    files_base_name = dir_name + \
                        '/input{}.txt'.format(file_index)

                    f = open(files_base_name, 'w')
                    f.write(line + '\n')
                    line_index = 2

        f.close()
        wdoc.save(file_name)

    elif file_name.lower().endswith('txt'):
        with open(file_name, 'r') as tfile:
            f = open(files_base_name, 'w')
            tfile_lines = tfile.readlines()
            for line in tfile_lines:
                strip_line = line.strip()

                if strip_line:

                    if line_index <= 25:
                        f.write(strip_line + '\n')
                        line_index = line_index + 1
                    else:
                        f.close()
                        file_index = file_index + 1
                        files_base_name = dir_name + \
                            '/input{}.txt'.format(file_index)

                        f = open(files_base_name, 'w')
                        f.write(strip_line + '\n')
                        line_index = 2
            f.close()

    else:
        print("File not supported :/ ")


def create_data_dir(directory):
    if not os.path.exists(directory):
        print("Creating data directory: " + directory)
        os.makedirs(directory)


def overseer(data_path, map_nodes, reduce_nodes):
    global failsafe_loop
    global map_failed
    global reduce_nodes_reset

    map_failed = False
    map_thread_pool = []
    reduce_thread_pool = []

    print("\n\n[{}] STARTING MAP-REDUCE...".format("OV"))
    data = os.listdir(data_path)

    if len(data) < map_nodes:
        map_nodes = len(data)

    chunk_size = math.floor(len(data) / map_nodes)
    chunks = [data[x:x+chunk_size] for x in range(0, len(data), chunk_size)]
    # print(chunks)

    results_path = "results/" + project_name + "/mapped"
    create_data_dir(results_path)

    thread_id = 1
    # syncronic implementation
    # for chunk in chunks:
    #     map_node(chunk, data_path, thread_id)
    #     thread_id = thread_id + 1
    # start map threads implementation
    for chunk in chunks:
        t = threading.Thread(target=map_node, args=[
            chunk, data_path, thread_id])
        t.start()
        map_thread_pool.append(t)
        thread_id = thread_id + 1

    for map_thread in map_thread_pool:
        map_thread.join()

    while map_failed and failsafe_loop != 5:
        failsafe_loop = failsafe_loop + 1
        print(
            "[{}] A MAP NODE FAILED ITS TASK. RESTARTING MAP NODES...".format("OV"))
        overseer(data_path, map_nodes, reduce_nodes)

    if failsafe_loop == 5:
        print(
            "[{}] /!\ MAPREDUCE COULD NOT BE COMPLETED DUE TO REPETITIVE MAP ERROR.".format("OV"))
        sys.exit(0)
    # end map 
    # start group by keys
    group_by_key(results_path)  
    # end group by keys
    grouped_files_path = "results/" + project_name + "/grouped"
    toreduce_path = "results/" + project_name + "/reduced"
    create_data_dir(toreduce_path)

    with open(grouped_files_path + "/toreduce.txt") as to_reduce_file:
        lines = to_reduce_file.readlines()
        if len(lines) != 0:
            # possible failure if lines in reduce files < reduce nodes
            chunk_size = math.floor(len(lines) / reduce_nodes)
            chunks = [lines[x:x+chunk_size]
                      for x in range(0, len(lines), chunk_size)]

            thread_id = 1
            # syncronic implementation
            # for chunk in chunks:
            #     # print(chunk)
            #     reducer_node(chunk, thread_id)
            #     thread_id = thread_id + 1
            # start reduce thread implementation
            for chunk in chunks:
                t = threading.Thread(target=reducer_node,
                                     args=[chunk, thread_id])
                t.start()
                reduce_thread_pool.append(t)
                thread_id = thread_id + 1

            for reduce_thread in reduce_thread_pool:
                reduce_thread.join()
        else:
            print("[{}] ALL WORDS WERE REDUCED IN GBK.".format("OV"))

    failsafe_loop = 1
    while len(reduce_nodes_reset) != 0 and failsafe_loop != 5:
        reduce_thread_pool = []
        failsafe_loop = failsafe_loop + 1
        for reset_node in reduce_nodes_reset:
            print("[{}] Restarting this reduce node.".format(reset_node))
            t = threading.Thread(target=reducer_node,
                                 args=[chunks[reset_node-1], reset_node])
            t.start()
            reduce_thread_pool.append(t)
            reduce_nodes_reset.remove(reset_node)

        for reduce_thread in reduce_thread_pool:
            reduce_thread.join()
              
    if failsafe_loop == 5:
        print(
            "[{}] /!\ MAPREDUCE COULD NOT BE COMPLETED DUE TO REPETITIVE REDUCE ERROR.".format("OV"))
        sys.exit(0)    

    print("[{}] MAP-REDUCE FINISHED, GATHERING VALUES...".format("OV"))
    create_data_dir("results/" + project_name + "/final")
    final_tuples_list = []

    reduced_file_zero = "results/" + project_name + "/grouped/reduced0.txt"
    with open(reduced_file_zero, 'r') as rfilezero:
        rlines = rfilezero.readlines()
        for rline in rlines:
            rline = rline.strip("<>\n").split(",")

            final_tuples_list.append((rline[0], int(rline[1])))

    reduced_data = "results/" + project_name + "/reduced/"

    rdata = os.listdir(reduced_data)
    for rfile in rdata:
        with open(reduced_data + rfile, 'r') as rfilesnumber:
            rlines = rfilesnumber.readlines()
            for rline in rlines:
                rline = rline.strip("<>\n").split(",")
                final_tuples_list.append((rline[0], int(rline[1])))

    print("[OV] Gathering reduced files finished. Total words indexed: ",
          len(final_tuples_list))

    # sort tuples 
    final_tuples_list.sort(key=lambda tup: tup[1], reverse=True)
    
    print("[{}] Saving final values in txt file...".format("OV"))

    final_file_path = "results/" + project_name + "/final/mapreduce.txt"

    with open(final_file_path, 'w') as fmapred:
        for kv_mr in final_tuples_list:
            fmapred.write("<{},{}>\n".format(kv_mr[0], kv_mr[1]))

    print("[{}] Done!\n".format("OV"))


def map_node(data_set, data_path, node_number):
    global map_failed

    try:
        print("Starting map node " + str(node_number) + " --------------")
        print("[{}] Mapping values...".format(node_number))
        chunk_map = {}

        mapped_file_name = "results/" + project_name + "/mapped"\
            "/mapped{}.txt".format(node_number)

        for fname in data_set:
            filename = os.fsdecode(data_path) + "/" + os.fsdecode(fname)
            with open(filename, 'r') as rfile:
                lines = rfile.readlines()

                for line in lines:
                    line = line.split()
                    for word in line:
                        word = re.sub(r'[^\w\s]', '', word)
                        if not word in chunk_map:
                            chunk_map[word] = 1
                        else:
                            chunk_map[word] = chunk_map[word] + 1

        max_key = max(chunk_map, key=chunk_map.get)
        print("[{}] The key with most entries was: <{} , {}>".format(
            node_number, max_key, chunk_map[max_key]))
        print("[{}] Writting mapped values to txt file...".format(node_number))

        with open(mapped_file_name, 'w') as mapfile:
            for key in chunk_map:
                mapfile.write("<{},{}>\n".format(key, chunk_map[key]))

        # if node_number == 5:
        #     raise Exception("map exception")

    except:
        print("[{}] /!\ WARNING: This map node failed its task.".format(node_number))
        map_failed = True


def group_by_key(data_path):
    print("[GBK] Grouping by keys...")

    group_dict = {}
    grouped_path = "results/" + project_name + "/grouped"
    create_data_dir(grouped_path)
    map_data = os.listdir(data_path)

    for mapfilename in map_data:
        mapfile_path = data_path + "/" + mapfilename

        with open(mapfile_path, 'r') as mapfile:
            lines = mapfile.readlines()

            for line in lines:
                # extract data from key,value arrange
                kv = line.strip("\n<>").split(",")

                if not kv[0] in group_dict:
                    group_dict[kv[0]] = [kv[1]]
                else:
                    group_dict[kv[0]].append(kv[1])

    max_key = max(group_dict, key=group_dict.get)
    print("[{}] Writting grouped key,values to txt file...".format("GBK"))

    already_reduced_file_name = grouped_path + "/reduced0.txt"
    to_reduce_file_name = grouped_path + "/toreduce.txt"

    with open(already_reduced_file_name, 'w') as reducedfile, open(to_reduce_file_name, 'w') as toreduce:
        for key in group_dict:
            if len(group_dict[key]) == 1:
                reducedfile.write("<{},{}>\n".format(key, group_dict[key][0]))
            else:
                toreduce.write("<{},{}>\n".format(key, group_dict[key]))


def reducer_node(data_set, node_number):
    global reduce_nodes_reset

    reduced_list = []
    print("Starting reduce node {} +---------------+".format(node_number))
    print("[{}] Reducing values...".format(node_number))

    try:
        for kvs in data_set:
            kvs = re.sub(r'[^\w\s]', " ", kvs).split()
            k = kvs[0]
            vs = kvs[1:]
            # reduce extracted values using python function powered by lambda expression
            reduced_value = reduce(lambda a, b: int(a) + int(b), vs)
            reduced_list.append((k, reduced_value))

        reduced_files_name = "results/" + project_name + \
            "/reduced/reduced{}.txt".format(node_number)

        print("[{}] Writting reduced values to txt file...".format(node_number))

        with open(reduced_files_name, 'w') as rfile:
            for kv in reduced_list:
                rfile.write("<{},{}>\n".format(kv[0], kv[1]))
                
        # if node_number == 2:
        #     raise Exception("reduce error")
    except:
        print("[{}] /!\ WARNING: This reduce node failed its task.".format(node_number))
        reduce_nodes_reset.append(node_number)

# program startup
setup_input("data/sherlock.txt")
data_directory = os.fsencode(dir_name)
overseer(data_directory, 6, 2)
