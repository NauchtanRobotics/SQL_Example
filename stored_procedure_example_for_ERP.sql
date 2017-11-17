DROP PROCEDURE IF EXISTS `current_inventory`;
#DROP TABLE IF EXISTS current_inventory_tbl;
DROP PROCEDURE IF EXISTS `forward_production_procedure`;
#DROP TABLE IF EXISTS forward_production_tbl;

DELIMITER $$
CREATE PROCEDURE `current_inventory` 
(  
 # IN - PASS STUFF HERE
 #OUT out1 			FLOAT,
 #OUT out2			FLOAT
 ) 

block1: BEGIN
DECLARE s_id, p_id, p_mass, l_stock, p_rcd, consumed, num_units_completed, num_parts_per_unit, manu_units, est_stock, cat INT default 0;

DECLARE p_name, s_name					VARCHAR(30);
DECLARE p_rev							VARCHAR(3);
DECLARE done, done_block2				BOOLEAN default FALSE;
DECLARE date_of_stock_take				DATE;
DECLARE stock_take_event				INT;
DECLARE product							INT;
              
DECLARE cursor1 CURSOR FOR SELECT part_id, part_description, part_mass_kg, part_revision_num, supplier_id, fab_category FROM Parts_tbl;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
DROP TABLE IF EXISTS current_inventory_tbl;
CREATE TEMPORARY TABLE current_inventory_tbl
  (
    part_id 					INT NOT NULL, 
    part_name					VARCHAR(30),
    part_mass_kg 				FLOAT,
    part_revision_num			VARCHAR(3),
	last_stock_take_qty			INT,
	qty_consumed_since_last_st	INT,
	qty_produced_since_last_st	INT,
    qty_purchased				INT,
    qty_current_stock_est 		INT,
    supplier_nm					VARCHAR(20),
    fab_cat						INT,
    supplier_id					INT
  )ENGINE=InnoDB;
  
  

# Iterates though each row of data in Parts_tbl
OPEN cursor1;
  
		read_loop: LOOP
			FETCH cursor1 INTO p_id, p_name, p_mass, p_rev, s_id, cat;
			IF done THEN
				LEAVE read_loop;
			END IF;
			
            # For the part of concern, determine the most recent relevant stock take event id
            
            SELECT max(stock_take_event_id)
				INTO 
							stock_take_event
				FROM 
							stock_take_detail_tbl 
				WHERE 
							stock_take_detail_tbl.part_id = p_id;
			
            # For the most recent stock take event associated with said part, determine the date of that last stock take
            
            IF stock_take_event IS NOT NULL THEN
					SELECT 
							max(stock_take_date)
					INTO 
							date_of_stock_take
					FROM 
							stock_take_event_tbl e
					WHERE 
							e.stock_take_event_id = stock_take_event;
                            
					
                    SELECT 	
							min(part_qty_in_stock)
					INTO 		
							l_stock
					FROM 
							stock_take_detail_tbl det
                    WHERE 
							det.part_id = p_id
					AND
							det.stock_take_event_id = stock_take_event;
                            
			ELSE
                SET date_of_stock_take = 1970-10-01;
                SET l_stock = 0;
			END IF;
            
				
			
            # Determine amount of parts received from direct purchasing
				SET p_rcd = NULL;
                
				SELECT 	
							sum(qty)
					INTO 		
							p_rcd
					FROM 
							parts_received_tbl r
                    WHERE 
							r.part_id = p_id
					AND
							r.received_date > date_of_stock_take;
			
            IF p_rcd IS NULL THEN
				SET p_rcd = 0;
			END IF;
            
            
            
            SELECT 	
							sum( consumed_qty )
					INTO 
							consumed
					FROM 	
							consumed_tbl ct						
					WHERE 
							ct.partnum = p_id					
					AND 
							ct.consumed_date > date_of_stock_take;
            
            IF consumed IS NULL THEN
				SET consumed = 0;
			END IF;                
                            
                            
			SELECT 	
							sum( manuf_qty )
					INTO 
							manu_units
					FROM 	
							manufacturing_done_tbl m						
					WHERE 
							m.part_id = p_id					
					AND 
							m.manuf_date > date_of_stock_take;
                            
                            
			IF manu_units IS NULL THEN
				SET manu_units = 0;
			END IF;
            
            SET est_stock :=  l_stock - consumed + manu_units + p_rcd;
            
            #IF est_stock < 0 THEN 
			#	SET est_stock := 0;
			#END IF;
                    
            INSERT INTO current_inventory_tbl ( part_id, part_name, part_mass_kg, part_revision_num, last_stock_take_qty, qty_consumed_since_last_st, qty_produced_since_last_st, qty_purchased, qty_current_stock_est, supplier_id, fab_cat )
                       VALUES
                       ( p_id, p_name, p_mass, p_rev, l_stock, consumed, manu_units, p_rcd, est_stock, s_id, cat );
    
			
            SET done = FALSE;
            
            
		END LOOP;

CLOSE cursor1;
	
#SELECT * FROM current_inventory_tbl;        
                

END block1;
$$



CREATE PROCEDURE `forward_production_procedure` 
(  
 # IN - PASS STUFF HERE
 #OUT out1 			FLOAT,
 #OUT out2			FLOAT
 ) 


block100: BEGIN

DECLARE p_id, num_parts_per_unit, num_open_orders, num_units_completed, demand_from_outstanding_orders, stock_est, required_manuf, required_pur, spl INT default 0;
DECLARE done, done_block200				BOOLEAN default FALSE;
DECLARE product							INT;
              
DECLARE cursor1 CURSOR FOR SELECT part_id FROM Parts_tbl;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

call current_inventory();
DROP TABLE IF EXISTS forward_production_tbl;
CREATE TEMPORARY TABLE forward_production_tbl
  (
    item_num					INT NOT NULL AUTO_INCREMENT,
    part_id 					INT NOT NULL, 
	assem_demand				INT,
    manuf_req	 				INT,
    purchase_req				INT,
    PRIMARY KEY ( item_num )
  )ENGINE=InnoDB;
  
  

# Iterate though each part in Parts_tbl	
OPEN cursor1;
  
		read_loop: LOOP
			FETCH cursor1 INTO p_id;
			IF done THEN
				LEAVE read_loop;
			END IF;
			
              
            SET done_block200 := FALSE;
            
            SET demand_from_outstanding_orders := 0;
            
            
            
            #For each part, iterate through the parts requirement for each product, as listed in product_parts_tbl
          
            block200: BEGIN
            
            DECLARE prd_id   INT;
            
			DECLARE curs200 CURSOR FOR SELECT prod_id FROM product_parts_tbl GROUP BY prod_id;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done_block200 = TRUE;
			
            
            OPEN curs200; 
			loop200: LOOP
			
            FETCH curs200 INTO product;   
				
                IF done_block200 THEN
					CLOSE curs200;
					LEAVE loop200;
				END IF; 
                
                
			# Find the number of units of product 'product' that have been ordered since the last stockstake
				SET num_open_orders := NULL;	
				
				SELECT 	
							sum( assembly_qty )
					INTO 
							num_open_orders
					FROM 	
							assembly_plan_tbl apt							
					WHERE 
							apt.prod_id = product					
					AND ( 
							apt.closed IS NULL 
						OR    apt.closed = FALSE
                        );
				
                
                IF(num_open_orders IS NULL) THEN
					SET num_open_orders := 0;
				END IF;
                							
                
                
    
				SET num_parts_per_unit := NULL;			
				
				SELECT 	
							sum(part_qty_in_product) 
					INTO 
							num_parts_per_unit
					FROM 	
							product_parts_tbl p						
					WHERE 
							p.prt_id = p_id					
					AND 
							p.prod_id = product;
				
                
                IF(num_parts_per_unit IS NULL) THEN
					SET num_parts_per_unit := 0;
				END IF;
                    
				SET demand_from_outstanding_orders := demand_from_outstanding_orders + num_open_orders * num_parts_per_unit;
                
			SET done_block200 = FALSE;
			
			END LOOP loop200;
			END block200;
            
            
            SELECT 
						sum(qty_current_stock_est) 
				INTO 
						stock_est
				FROM 
						current_inventory_tbl c 
				WHERE 
						c.part_id = p_id;
                        
                        
                        
			SELECT
						max( supplier_id )
				INTO
						spl
				FROM
						Parts_tbl p
				WHERE
						p.part_id = p_id;
				
                
           # spl = 0 means in-house fabrication of part     
            IF spl = 0 THEN
				SET required_manuf = demand_from_outstanding_orders - stock_est;
                SET required_pur   = 0;
			ELSE
				SET required_manuf = 0;
                SET required_pur   = demand_from_outstanding_orders - stock_est;
            END IF;
            
            
            IF required_manuf < 0 THEN
				SET required_manuf = 0;
			END IF;
            
			IF required_pur < 0 THEN
				SET required_pur :=0;
			END IF;
    
            INSERT INTO forward_production_tbl ( part_id,  assem_demand, manuf_req, purchase_req )
                       VALUES
                       ( p_id, demand_from_outstanding_orders, required_manuf, required_pur );
    
			
            SET done = FALSE;
            
            
		END LOOP;

CLOSE cursor1;
	
#SELECT a.part_id, a.assem_demand, a.manuf_req, a.purchase_req #b.qty_current_stock_est, can't access current_inventory_tbl here until the procedure has been run
	#FROM forward_production_tbl a; #,  b current_inventory_tbl
   # WHERE a.part_id = b.part_id;    
                
     
END block100;
$$



call forward_production_procedure();
  
    
	