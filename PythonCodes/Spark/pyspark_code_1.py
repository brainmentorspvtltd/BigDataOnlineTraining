Python 3.8.8 (tags/v3.8.8:024d805, Feb 19 2021, 13:18:16) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license()" for more information.
>>> import os, sys
>>> import pyspark
>>> os.environ['PYSPARK_PYTHON'] = sys.executable
>>> os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
>>> from pyspark import SparkContext
>>> sc = SparkContext("local", "App_1")
>>> x = sc.parallelize([("Python",1),("Java",3)])
>>> y = sc.parallelize([("Python",2),("Java",4)])
>>> z = x.join(y)
>>> z.collect()
[('Python', (1, 2)), ('Java', (3, 4))]
>>> rdd = sc.parallelize([],5)
>>> rdd
ParallelCollectionRDD[10] at readRDDFromFile at PythonRDD.scala:274
>>> rdd.getNumPartitions()
5
>>> x.getNumPartitions()
1
>>> file = sc.textFile("F:/Batches/ui_notes.txt")
>>> file.count()
52
>>> file.collect()
['HTML 4', '- Static web page', '- structure of the page', '', 'w3 - world wide web', '', '1. emmet', '2. check on w3 validator', '3. plugin of w3c validtor in vs code', '', 'SEO - Search Engine Optimization', '', 'CSS 2', '- styling', '', 'HTML 5', '- Dynamic HTML', '- Multimedia', '- Form Validation', '- Canvas', '', 'CSS 3', '- Animations - 2D + 3D', '- RWD - Responsive Web Design/Development', '', '', 'CSS - Cascading Style Sheet', '', 'Basic Syntax', 'Selectors', 'Borders and Backgrounds', 'Text and Font', '=====================', 'Floating', 'Positions', 'Display', 'Pseudo classes and elements', '=====================', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
>>> file.flatMap(lambda x : x.split(" ")).collect()
['HTML', '4', '-', 'Static', 'web', 'page', '-', 'structure', 'of', 'the', 'page', '', 'w3', '-', 'world', 'wide', 'web', '', '1.', 'emmet', '2.', 'check', 'on', 'w3', 'validator', '3.', 'plugin', 'of', 'w3c', 'validtor', 'in', 'vs', 'code', '', 'SEO', '-', 'Search', 'Engine', 'Optimization', '', 'CSS', '2', '-', 'styling', '', 'HTML', '5', '-', 'Dynamic', 'HTML', '-', 'Multimedia', '-', 'Form', 'Validation', '-', 'Canvas', '', 'CSS', '3', '-', 'Animations', '-', '2D', '+', '3D', '-', 'RWD', '-', 'Responsive', 'Web', 'Design/Development', '', '', 'CSS', '-', 'Cascading', 'Style', 'Sheet', '', 'Basic', 'Syntax', 'Selectors', 'Borders', 'and', 'Backgrounds', 'Text', 'and', 'Font', '=====================', 'Floating', 'Positions', 'Display', 'Pseudo', 'classes', 'and', 'elements', '=====================', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
>>> file = sc.textFile("F:/count_vehciles.txt")
>>> rdd = file.flatMap(lambda x : x.split(','))
>>> rdd_2 = rdd.map(lambda x : (x,1))
>>> rdd_2.collect()
[('car', 1), ('bike', 1), ('car', 1), ('car', 1), ('bike', 1), ('bus', 1), ('bus', 1), ('truck', 1), ('bus', 1), ('truck', 1), ('truck', 1), ('bus', 1), ('car', 1), ('bike', 1), ('bike', 1), ('bike', 1), ('bike', 1), ('truck', 1), ('bus', 1)]
>>> rdd_2.reduceByKey(lambda a, b : a + b).collect()
[('car', 4), ('bike', 6), ('bus', 5), ('truck', 4)]
>>> rdd_3 = rdd_2.reduceByKey(lambda a, b : a + b)
>>> rdd_3.map(lambda x : (x[1], x[0])).sortByKey().collect()
[(4, 'car'), (4, 'truck'), (5, 'bus'), (6, 'bike')]
>>> rdd_3.max()
('truck', 4)
>>> rdd_3.map(lambda x : (x[1], x[0])).max().collect()
Traceback (most recent call last):
  File "<pyshell#26>", line 1, in <module>
    rdd_3.map(lambda x : (x[1], x[0])).max().collect()
AttributeError: 'tuple' object has no attribute 'collect'
>>> rdd_3.map(lambda x : (x[1], x[0])).max()
(6, 'bike')
>>> file = sc.textFile('F:/file_1.txt')
>>> file.map(lambda x : x.split(" ")).collect()
[['this', 'is', 'hadoop'], ['hadoop', 'is', 'no', 'longer', 'used'], ['spark', 'is', 'better', 'to', 'use']]
>>> file.flatMap(lambda x : x.split(" ")).collect()
['this', 'is', 'hadoop', 'hadoop', 'is', 'no', 'longer', 'used', 'spark', 'is', 'better', 'to', 'use']
>>> 