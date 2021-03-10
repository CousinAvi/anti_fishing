DELIMITER //
CREATE PROCEDURE sql_delete(in sometext varchar(50))
begin
	declare i int;
    set i = (select count(*) from final_table where domen = sometext);
    delete from final_table where domen = sometext limit i;
end