import os
import psycopg2 as psycopg
import io

# database connections
src_conn = None
target_conn = None

# debug flag
DEBUG = False


# Clones the given table without any modifications
def copy_verbatim(name):
    clone(name, name)
    pass


# Clones the `thing_location` table into `platform_location` (basically renaming the table)
def copy_thing_location(name):
    if name == "thing_location":
        clone("thing_location", "platform_location")
    pass


# Copies parameters into their respective tables
# in
def copy_parameters(name):
    src_cursor = src_conn.cursor()
    target_cursor = target_conn.cursor()

    cfg = {
        "observation_parameters": {
            "name": "observation_parameter",
            "key": "fk_observation_id"
        }
    }

    src_cursor.execute("SELECT * FROM {} LEFT JOIN parameter on parameter_id = fk_parameter_id".format(name))
    params = src_cursor.fetchall()

    for p in params:
        statement = "INSERT INTO public.{}(parameter_id, type, name, description, last_update, domain, {}, " \
                    "fk_parent_parameter_id, value_boolean, value_category, fk_unit_id, value_count, value_quantity, " \
                    "value_text, value_xml, value_json, value_temporal_from, value_temporal_to) VALUES (nextval(" \
                    "'parameter_seq'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);".format(
            cfg[name]["name"],
            cfg[name]["key"]
        )

        target_cursor.execute(statement, (
            p[3],  # type
            p[4],  # name
            "",  # description
            p[5],  # last_update
            p[6],  # domain
            p[0],  # foreign key
            None,  # parent parameter
            p[7],  # value_boolean
            p[8],  # value_category
            p[9],  # fk_unit_id
            p[10],  # value_count
            p[11],  # value_quantity
            p[12],  # value_text
            p[13],  # value_xml
            p[14],  # value_json
            None,  # value_temporal_from
            None,  # value_temporal_to
        ))
    target_conn.commit()


# Copies Observations in blocks of 100_000 entries.
# Restores `fk_dataset_first_obs` & `fk_dataset_last_obs` Constraints on public.dataset as they are now fulfilled.
def copy_observations(name):
    src_cursor = src_conn.cursor()
    target_cursor = target_conn.cursor()

    src_cursor.execute("SELECT COUNT(*) FROM public.observation;")
    total = src_cursor.fetchone()[0]

    for i in range(0, total, 100000):
        dump = io.StringIO()

        src_cursor.copy_expert("COPY (SELECT observation_id, value_type, fk_dataset_id, sampling_time_start, "
                               "sampling_time_end, result_time, identifier, sta_identifier, "
                               "fk_identifier_codespace_id, name, fk_name_codespace_id, description, is_deleted, "
                               "valid_time_start, valid_time_end, sampling_geometry, value_identifier, value_name, "
                               "value_description, vertical_from, vertical_to, fk_parent_observation_id, "
                               "value_quantity, value_text, value_count, value_category, value_boolean, "
                               "detection_limit_flag, detection_limit, value_reference, value_geometry, value_array, "
                               "fk_result_template_id FROM public.observation LIMIT 100000 OFFSET {}) TO "
                               "STDOUT".format(i), dump)
        src_conn.commit()
        dump.seek(0)

        target_cursor.copy_expert("COPY public.observation FROM STDIN", dump)
        target_conn.commit()

    # Restore previously dropped constraints on datasets
    target_cursor.execute(
        "ALTER TABLE public.dataset ADD CONSTRAINT fk_dataset_first_obs FOREIGN KEY (fk_first_observation_id) "
        "REFERENCES public.observation (observation_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION")
    target_cursor.execute(
        "ALTER TABLE public.dataset ADD CONSTRAINT fk_dataset_last_obs FOREIGN KEY (fk_last_observation_id) "
        "REFERENCES public.observation (observation_id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION")


# Merges public.dataset & public.datastream & public.datastream_dataset into single public.dataset
# Refactors datastream into aggregate dataset.
# Drops fk_dataset_first_obs` & `fk_dataset_last_obs` Constraints as they prevent insertion before observations are inserted
def copy_dataset(name):
    src_cursor = src_conn.cursor()
    target_cursor = target_conn.cursor()

    # lift constraints that prevent us from inserting fk_dataset_first_obs
    target_cursor.execute("ALTER TABLE public.dataset DROP CONSTRAINT IF EXISTS fk_dataset_first_obs")
    target_cursor.execute("ALTER TABLE public.dataset DROP CONSTRAINT IF EXISTS fk_dataset_last_obs")

    src_cursor.execute("SELECT * FROM public.dataset")
    datasets = src_cursor.fetchall()

    ## Copy underlying datasets
    for p in datasets:
        statement = "INSERT INTO public.dataset(dataset_id, discriminator, identifier, sta_identifier, name, " \
                    "description, first_time, last_time, result_time_start, result_time_end, observed_area, " \
                    "fk_procedure_id, fk_phenomenon_id, fk_offering_id, fk_category_id, fk_feature_id, " \
                    "fk_platform_id, fk_unit_id, fk_format_id, fk_aggregation_id, first_value, last_value, " \
                    "fk_first_observation_id, fk_last_observation_id, dataset_type, observation_type, value_type, " \
                    "is_deleted, is_disabled, is_published, is_mobile, is_insitu, is_hidden, origin_timezone, " \
                    "decimals, fk_identifier_codespace_id, fk_name_codespace_id, fk_value_profile_id) VALUES (%s, %s," \
                    " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                    " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); "
        target_cursor.execute(statement, (
            p[0], None, p[26], None, p[28], p[30], p[19], p[20], None, None, None, p[4], p[5], p[6], p[7], p[8], p[9],
            p[11], p[10], None, p[21], p[22], p[23], p[24], p[1], p[2], p[3], p[12], p[13], p[14], p[15], p[16], p[17],
            p[18], p[25], p[27], p[29], p[31],))
    target_conn.commit()

    src_cursor.execute("SELECT * FROM public.datastream")
    datastreams = src_cursor.fetchall()

    for d in datastreams:
        print(d)

        # Get offering
        src_cursor.execute(
            "SELECT fk_offering_id from public.dataset "
            "inner join public.datastream_dataset on dataset_id = fk_dataset_id where fk_datastream_id = {}".format(
                d[0]))
        fk_offering = src_cursor.fetchone()[0]

        target_cursor.execute("SELECT MAX(dataset_id)+1 FROM public.dataset")
        datastream_id = target_cursor.fetchone()[0]

        statement = "INSERT INTO public.dataset(dataset_id, discriminator, identifier, sta_identifier, name, " \
                    "description, observed_area, result_time_start, result_time_end, fk_format_id, fk_unit_id, " \
                    "fk_platform_id, fk_procedure_id, fk_phenomenon_id, fk_offering_id, fk_category_id) VALUES (" \
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s); "
        target_cursor.execute(statement, (
            datastream_id, "aggregation", d[1], d[2], d[3], d[14], d[4], d[5], d[6], d[7], d[10], d[11], d[12], d[13],
            fk_offering, 1
        ))

        # link sub-datasets to aggregations
        target_cursor.execute("SELECT dataset_id FROM public.dataset WHERE sta_identifier = '{}'".format(d[2]))
        aggregation_id = target_cursor.fetchone()[0]

        src_cursor.execute(
            "SELECT fk_dataset_id from public.datastream_dataset where fk_datastream_id = {}".format(d[0]))
        sub_dataset_ids = ",".join([str(x[0]) for x in src_cursor.fetchall()])

        target_cursor.execute(
            "UPDATE public.dataset SET fk_aggregation_id = {} WHERE dataset_id in ({})".format(
                aggregation_id,
                sub_dataset_ids)
        )
        target_conn.commit()


# Copies public.platform
def copy_platform(name):
    dump = io.StringIO()
    src_cursor = src_conn.cursor()
    src_cursor.copy_expert(
        "COPY public.platform(platform_id, identifier, sta_identifier, fk_identifier_codespace_id, name, "
        "fk_name_codespace_id, description) TO STDOUT".format(
            name), dump)
    src_conn.commit()
    dump.seek(0)

    if DEBUG:
        print(dump.getvalue())

    target_cursor = target_conn.cursor()
    target_cursor.copy_expert(
        "COPY public.platform(platform_id, identifier, sta_identifier, fk_identifier_codespace_id, name, "
        "fk_name_codespace_id, description) FROM STDIN",
        dump)
    target_conn.commit()

    # TODO: Platform->properties



# Reorder sta_identifier column
def copy_feature(name):
    clone("feature",
          "feature",
          "COPY public.feature(feature_id, discriminator, fk_format_id, identifier, sta_identifier, "
          "fk_identifier_codespace_id, name, fk_name_codespace_id, description, xml, url, geom) TO STDOUT")

# Reorder sta_identifier column
def copy_location(name):
    clone("location",
          "location",
          "COPY public.location(location_id, identifier, sta_identifier, name, description, location, "
          "geom, fk_format_id) TO STDOUT")

# Reorder sta_identifier column
def copy_procedure(name):
    clone("procedure",
          "procedure",
          "COPY public.procedure(procedure_id, identifier, sta_identifier, fk_identifier_codespace_id, name, "
          " fk_name_codespace_id, description, description_file, is_reference, fk_type_of_procedure_id, is_aggregation, "
          "fk_format_id) TO STDOUT")

def update_sequences():
    target_cursor = target_conn.cursor()
    for seq in sequences:
        target_cursor.execute("select max({})+1 from {}".format(seq + "_id", seq))
        val = target_cursor.fetchone()[0]
        if val is None:
            continue
        print("updating sequence {}. Setting to {}".format(seq, val))
        target_cursor.execute("select setval('{}',{});".format(seq + "_seq", val))
    target_conn.commit()


# Clones given src_table into target_table. Uses `COPY` semantics for efficiency.
# Requires both tables to have the same column definitions
def clone(src_table, target_table, src_copy="COPY {} TO STDOUT", target_copy="COPY {} FROM STDIN"):
    dump = io.StringIO()

    src_cursor = src_conn.cursor()
    src_cursor.copy_expert(src_copy.format(src_table), dump)
    src_conn.commit()
    dump.seek(0)

    if DEBUG:
        print(dump.getvalue())

    target_cursor = target_conn.cursor()
    target_cursor.copy_expert(target_copy.format(target_table), dump)
    target_conn.commit()


# Truncates all tables in target_db.
def truncate_tables():
    print("Clearing target Database")
    cur = target_conn.cursor()

    # We have no "TRUNCATE ALL" so we generate individual statements
    cur.execute(
        "SELECT 'TRUNCATE TABLE ' ||  tablename || ' RESTART IDENTITY CASCADE;' FROM pg_tables WHERE schemaname='public';")
    for stmnt in cur.fetchall():
        if DEBUG:
            print(stmnt[0])
        cur.execute(stmnt[0])
    target_conn.commit()


## Hardcoded Table Names
tables = {
    "category": copy_verbatim,
    "format": copy_verbatim,
    "category_i18n": copy_verbatim,
    "composite_phenomenon": copy_verbatim,
    "dataset_reference": copy_verbatim,
    "feature": copy_feature,
    "feature_hierarchy": copy_verbatim,
    "historical_location": copy_verbatim,
    "location": copy_location,
    "location_historical_location": copy_verbatim,
    "location_i18n": copy_verbatim,
    "observation_i18n": copy_verbatim,
    "offering": copy_verbatim,
    "offering_feature_type": copy_verbatim,
    "offering_hierarchy": copy_verbatim,
    "offering_i18n": copy_verbatim,
    "offering_observation_type": copy_verbatim,
    "offering_related_feature": copy_verbatim,
    "phenomenon": copy_verbatim,
    "phenomenon_i18n": copy_verbatim,
    "platform_i18n": copy_verbatim,
    "procedure": copy_procedure,
    "procedure_hierarchy": copy_verbatim,
    "procedure_i18n": copy_verbatim,
    "procedure_history": copy_verbatim,
    "related_dataset": copy_verbatim,
    "related_feature": copy_verbatim,
    "related_observation": copy_verbatim,
    "unit": copy_verbatim,
    "unit_i18n": copy_verbatim,
    "value_profile_i18n": copy_verbatim,
    "value_profile": copy_verbatim,

    "platform": copy_platform,

    "dataset": copy_dataset,
    "observation": copy_observations,

    "dataset_i18n": None,
    "dataset_parameter": None,
    "datastream": None,
    "datastream_dataset": None,
    "datastream_i18n": None,
    "parameter": None,

    "procedure_parameter": None,
    "result_template": None,
    "tag": None,
    "tag_dataset": None,
    "tag_i18n": None,
    "value_blob": None,

    "platform_location": copy_thing_location,
    "thing_location": copy_thing_location,

    "observation_parameters": copy_parameters,
    "platform_parameter": copy_parameters,
    "feature_parameter": None,
    "location_parameter": None,
    "observation_parameter": None,
    "phenomenon_parameter": None,
}

sequences = [
    "category",
    "codespace",
    "dataset",
    "feature",
    "format",
    "historical_location",
    "location",
    "observation",
    "offering",
    "phenomenon",
    "platform",
    "procedure",
    "unit",
]


def main():
    global DEBUG

    src = os.getenv("SRC_DB", "host=localhost, dbname=sta user=postgres password=postgres port=5001")
    target = os.getenv("TARGET_DB", "host=localhost, dbname=sta user=postgres password=postgres port=5000")
    DEBUG = os.getenv("debug", "") != ""

    # Connect to databases
    with psycopg.connect(src) as s, psycopg.connect(target) as t:
        global src_conn, target_conn
        src_conn = s
        target_conn = t

        # Truncate target db
        truncate_tables()

        # Copy tables
        for name in tables:
            if tables[name] is not None:
                print("copying {}".format(name))
                tables[name](name)
            else:
                print("ignoring {}".format(name))
                pass
        # Update sequences
        update_sequences()


if __name__ == '__main__':
    main()
