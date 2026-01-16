        CREATE TABLE tpcch.warehouse (
            w_id integer,
            w_name char(10),
            w_street_1 char(20),
            w_street_2 char(20),
            w_city char(20),
            w_state char(2),
            w_zip char(9),
            w_tax decimal(4,4),
            w_ytd decimal(12,2),
            PRIMARY KEY (w_id)
        );

        CREATE TABLE tpcch.district (
            d_id tinyint,
            d_w_id integer,
            d_name char(10),
            d_street_1 char(20),
            d_street_2 char(20),
            d_city char(20),
            d_state char(2),
            d_zip char(9),
            d_tax decimal(4,4),
            d_ytd decimal(12,2),
            d_next_o_id integer,
            PRIMARY KEY (d_w_id, d_id)
        );



        CREATE TABLE tpcch.customer (
            c_id smallint,
            c_d_id tinyint,
            c_w_id integer,
            c_first char(16),
            c_middle char(2),
            c_last char(16),
            c_street_1 char(20),
            c_street_2 char(20),
            c_city char(20),
            c_state char(2),
            c_zip char(9),
            c_phone char(16),
            c_since DATE,
            c_credit char(2),
            c_credit_lim decimal(12,2),
            c_discount decimal(4,4),
            c_balance decimal(12,2),
            c_ytd_payment decimal(12,2),
            c_payment_cnt smallint,
            c_delivery_cnt smallint,
            c_data text,
            c_n_nationkey integer,
            PRIMARY KEY(c_w_id, c_d_id, c_id)
        );


        CREATE TABLE tpcch.history (
            h_c_id smallint,
            h_c_d_id tinyint,
            h_c_w_id integer,
            h_d_id tinyint,
            h_w_id integer,
            h_date date,
            h_amount decimal(6,2),
            h_data char(24)
        );


        CREATE TABLE tpcch.neworder (
        	no_o_id integer,
        	no_d_id tinyint,
        	no_w_id integer,
        	PRIMARY KEY (no_w_id, no_d_id, no_o_id)
        );

        CREATE TABLE tpcch.orders (
        	o_id integer,
        	o_d_id tinyint,
        	o_w_id integer,
        	o_c_id smallint,
        	o_entry_d date,
        	o_carrier_id tinyint,
        	o_ol_cnt tinyint,
        	o_all_local tinyint,
        	PRIMARY KEY (o_w_id, o_d_id, o_id)
        );



        CREATE TABLE tpcch.orderline (
        	ol_o_id integer,
        	ol_d_id tinyint,
        	ol_w_id integer,
        	ol_number tinyint,
        	ol_i_id integer,
        	ol_supply_w_id integer,
        	ol_delivery_d date,
        	ol_quantity smallint,
        	ol_amount decimal(6,2),
        	ol_dist_info char(24),
        	PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
        );



        CREATE TABLE tpcch.item (
        	i_id integer,
        	i_im_id smallint,
        	i_name char(24),
        	i_price decimal(5,2),
        	i_data char(50),
        	PRIMARY KEY (i_id)
        );

        CREATE TABLE tpcch.stock (
        	s_i_id integer,
        	s_w_id integer,
        	s_quantity integer,
        	s_dist_01 char(24),
    	    s_dist_02 char(24),
        	s_dist_03 char(24),
        	s_dist_04 char(24),
        	s_dist_05 char(24),
        	s_dist_06 char(24),
        	s_dist_07 char(24),
        	s_dist_08 char(24),
        	s_dist_09 char(24),
        	s_dist_10 char(24),
        	s_ytd integer,
        	s_order_cnt integer,
        	s_remote_cnt integer,
        	s_data char(50),
        	s_su_suppkey integer,
        	PRIMARY KEY (s_w_id, s_i_id)
        );
    

        CREATE TABLE tpcch.nation (
        	n_nationkey tinyint NOT NULL,
        	n_name char(25) NOT NULL,
        	n_regionkey tinyint NOT NULL,
        	n_comment char(152) NOT NULL,
        	PRIMARY KEY (n_nationkey)
        );

        CREATE TABLE tpcch.supplier (
        	su_suppkey smallint NOT NULL,
    	    su_name char(25) NOT NULL,
        	su_address char(40) NOT NULL,
        	su_nationkey tinyint NOT NULL,
        	su_phone char(15) NOT NULL,
        	su_acctbal decimal(12,2) NOT NULL,
        	su_comment char(101) NOT NULL,
        	PRIMARY KEY (su_suppkey)
        );

        CREATE TABLE tpcch.region (
        	r_regionkey tinyint NOT NULL,
        	r_name char(55) NOT NULL,
        	r_comment char(152) NOT NULL,
            PRIMARY KEY (r_regionkey)
        );