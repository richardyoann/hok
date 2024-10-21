create sequence if not exists use_case_specif.fct_hcal_dfa_hok_calc_id_calc_seq
	increment by 1
	minvalue 1
	maxvalue 2147483647
	start 1
	cache 1
	no cycle;

create sequence if not exists use_case_specif.fct_hcal_dfa_hok_calc_id_groupe_seq
	increment by 1
	minvalue 1
	maxvalue 2147483647
	start 1
	cache 1
	no cycle;

create sequence if not exists use_case_specif.fct_hcal_dfa_hok_mailles_id_maille_seq
	increment by 1
	minvalue 1
	maxvalue 2147483647
	start 1
	cache 1
	no cycle;

create table if not exists use_case_specif.hcal_dfa_hok_calc (
	id_calc int4 NOT NULL DEFAULT nextval('use_case_specif.fct_hcal_dfa_hok_calc_id_calc_seq'::regclass),
	id_parent int4 NOT NULL,
	"label" varchar(50) NOT NULL,
	rapports _text NULL,
	id_maille_groupe int4 NULL,
	cron_jours varchar(50) NULL,
	cron_mois varchar(50) NULL,
	CONSTRAINT hcal_dfa_hok_calc_pkey PRIMARY KEY (id_calc)--,
--	CONSTRAINT hcal_dfa_hok_calc_fk FOREIGN KEY (id_parent) REFERENCES use_case_specif.hcal_dfa_hok_calc(id_calc)
);

insert into use_case_specif.hcal_dfa_hok_calc (id_calc, id_parent, "label", rapports, id_maille_groupe, cron_jours, cron_mois)
values (0, 0, 'racine', null, null, null, null);

alter table use_case_specif.hcal_dfa_hok_calc
add constraint hcal_dfa_hok_calc_fk FOREIGN KEY (id_parent) REFERENCES use_case_specif.hcal_dfa_hok_calc(id_calc);


comment on table use_case_specif.hcal_dfa_hok_calc is 'Table regroupant les ids des indicateurs ainsi que leurs libellés';
-- Column comments
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.id_calc IS 'Identifiant du PKI';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.id_parent IS 'Reference à l identifiant du PKI parent;
Cela permet de decouper le pki en plusieurs sous PKI. 
Exemple : PKI OS peut etre decoupe en OS Windows 7, OS Windows 8,OS Windows 10, OS Windows 11;
FK sur id_calc ';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.label IS 'Label du calcul';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.id_maille_groupe IS 'Identifiant du groue de mailles';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.rapports IS 'Tableau de caractere permettant de lister l ensemble des rapports referencant le PKI';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.cron_jours IS 'Chaine de caractere au format cron permettant d indiquer la temporalite du lancement
Exemple : tous les jours à 15h : 15 00  * * * ';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_calc.cron_mois IS 'Chaine de caractere au format cron permettant d indiquer la temporalite du lancement mensuel 
Exemple : tous les 1 er de chaque mmois à 8h 00 08  01 * * ';


create table if not exists use_case_specif.hcal_dfa_hok_mailles (
	id_maille int4 not null default nextval('use_case_specif.fct_hcal_dfa_hok_mailles_id_maille_seq'::regclass),
	"label" varchar(100) not null,
	id_parent int4 not null,
	constraint hcal_dfa_hok_mailles_pkey primary key (id_maille)
);
comment on table use_case_specif.hcal_dfa_hok_mailles is 'Table regroupant les ids des mailles ainsi que leurs libellés';
-- Column comments
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_mailles.id_maille IS 'Identifiant de la maille d un PKI';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_mailles.id_parent IS 'Reference à l identifiant du maille parent;
Cela permet de decouper le pki en plusieurs sous maille.';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_mailles.label IS 'Label de la maille';

insert into use_case_specif.hcal_dfa_hok_mailles (id_maille, "label", id_parent)
values (0, 'racine', 0);



create table if not exists use_case_specif.hcal_dfa_hok_groupes_mailles (
	id_groupe int4 not null default nextval('use_case_specif.fct_hcal_dfa_hok_calc_id_groupe_seq'::regclass),
	"label" varchar(100) not null,
	id_parent int4 not null,
	constraint hcal_dfa_hok_groupes_mailles_pkey primary key (id_groupe)
);
comment on table use_case_specif.hcal_dfa_hok_groupes_mailles is 'Table regroupant les groupes de maille ainsi que leurs libellés';
-- Column comments
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_groupes_mailles.id_groupe IS 'Identifiant du groupe de maille d un PKI';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_groupes_mailles.id_parent IS 'Reference à l identifiant du groupe maille parent;
Cela permet de decouper le pki en plusieurs sous maille.';
COMMENT ON COLUMN use_case_specif.hcal_dfa_hok_groupes_mailles.label IS 'Label du groupe de maille';


create table if not exists use_case_specif.hcal_dfa_hok_jours (
	id_calc int4 not null,
	id_maille int4 not null,
	"date" date not null,
	valeur int4 not null,
	constraint hcal_dfa_hok_jours_pkey primary key (id_calc,id_maille,date)
);
comment on table use_case_specif.hcal_dfa_hok_jours is 'Table permettant d historiser les resultats des PKI en fonction de sa programmation durant un mois.
Le cumul de chaque PKI sera sauvegarder automatique chaque mois dans la table hcal_dfa_hok_mois puis elle sera videe chaque debut de mois.
L unicite sera faite en fonction des id calc et maille et de la date du calcul du pki.';


create table if not exists use_case_specif.hcal_dfa_hok_mois (
	id_calc int4 not null ,
	id_maille int4 not null ,
	"date" date not null,
	valeur int4 not null,
	constraint hcal_dfa_hok_mois_pkey primary key (id_calc,id_maille,date)
);
comment on table use_case_specif.hcal_dfa_hok_mois is 'Table permettant d historiser le resutat mensuels des pki. 
L unicite sera faite en fonction des id calc et maille et du mois d historisation';

-- Permissions
--hcal_dfa_hok_calc
ALTER TABLE use_case_specif.hcal_dfa_hok_calc OWNER TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_calc TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_calc TO dataeng;

--hcal_dfa_hok_groupes_mailles
ALTER TABLE use_case_specif.hcal_dfa_hok_groupes_mailles OWNER TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_groupes_mailles TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_groupes_mailles TO dataeng;

--hcal_dfa_hok_mailles
ALTER TABLE use_case_specif.hcal_dfa_hok_mailles OWNER TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_mailles TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_mailles TO dataeng;

--hcal_dfa_hok_jours
ALTER TABLE use_case_specif.hcal_dfa_hok_jours OWNER TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_jours TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_jours TO dataeng;

--hcal_dfa_hok_mois
ALTER TABLE use_case_specif.hcal_dfa_hok_mois OWNER TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_mois TO dataanalyst;
GRANT ALL ON TABLE use_case_specif.hcal_dfa_hok_mois TO dataeng;


--drop table use_case_specif.hcal_dfa_hok_mois;
--drop table use_case_specif.hcal_dfa_hok_jours;
--drop table use_case_specif.hcal_dfa_hok_mailles;
--drop table use_case_specif.hcal_dfa_hok_calc;
--commit;
