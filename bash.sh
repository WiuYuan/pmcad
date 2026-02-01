nohup python -m test.get_llm_relation > ../logs/get_llm_relation.out 2>&1 &

nohup python -m test.ontology_decomposition > ../logs/ontology_decomposition.out 2>&1 &

nohup python -m test.map_ontology.map_all_ontology > ../logs/map_all_ontology.out 2>&1 &
nohup python -m test.map_ontology.map_all_ontology_net > ../logs/map_all_ontology_net.out 2>&1 &


nohup python -m test.map_ontology.get_final > ../logs/get_final.out 2>&1 &
nohup python -m test.llm_judge_relation > ../logs/llm_judge_relation.out 2>&1 &