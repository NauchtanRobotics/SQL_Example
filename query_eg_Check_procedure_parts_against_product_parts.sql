SELECT t2.prt_id, t2.pparts_cnt, t1.procedure_cnt, part_description FROM 
(SELECT part_num, count(part_num) AS procedure_cnt 
FROM ass_procedures_tbl 
WHERE prod_id = 27
AND part_num != 0 
AND part_num IS NOT NULL 
GROUP BY part_num 
ORDER BY part_num) AS t1 
RIGHT JOIN 
(SELECT prt_id, part_qty_in_product AS pparts_cnt 
FROM product_parts_tbl 
WHERE prod_id = 27 
AND prt_id != 0 
AND prt_id IS NOT NULL 
GROUP BY prt_id 
ORDER BY prt_id) AS t2 
ON t1.part_num = t2.prt_id 
RIGHT JOIN Parts_tbl ON part_id = t2.prt_id 
WHERE t1.procedure_cnt != t2.pparts_cnt 
OR (t1.procedure_cnt IS NULL XOR t2.pparts_cnt IS NULL);