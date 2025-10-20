--
-- PostgreSQL database dump
--

-- Dumped from database version 15.9 (Debian 15.9-1.pgdg120+1)
-- Dumped by pg_dump version 17.0

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: external; Type: SCHEMA; Schema: -; Owner: admin
--

CREATE SCHEMA external;


ALTER SCHEMA external OWNER TO admin;

--
-- Name: log_level; Type: TYPE; Schema: public; Owner: admin
--

CREATE TYPE public.log_level AS ENUM (
    'ERROR',
    'WARN',
    'INFO',
    'DEBUG',
    'TRACE'
);


ALTER TYPE public.log_level OWNER TO admin;

--
-- Name: queue_type; Type: TYPE; Schema: public; Owner: admin
--

CREATE TYPE public.queue_type AS ENUM (
    'io.kestra.core.models.executions.Execution',
    'io.kestra.core.models.flows.FlowInterface',
    'io.kestra.core.models.templates.Template',
    'io.kestra.core.models.executions.ExecutionKilled',
    'io.kestra.core.runners.WorkerJob',
    'io.kestra.core.runners.WorkerTaskResult',
    'io.kestra.core.runners.WorkerInstance',
    'io.kestra.core.runners.WorkerTaskRunning',
    'io.kestra.core.models.executions.LogEntry',
    'io.kestra.core.models.triggers.Trigger',
    'io.kestra.core.models.executions.MetricEntry',
    'io.kestra.core.runners.WorkerTriggerResult',
    'io.kestra.core.runners.SubflowExecutionResult',
    'io.kestra.core.server.ClusterEvent',
    'io.kestra.core.runners.SubflowExecutionEnd',
    'io.kestra.core.runners.ExecutionRunning'
);


ALTER TYPE public.queue_type OWNER TO admin;

--
-- Name: state_type; Type: TYPE; Schema: public; Owner: admin
--

CREATE TYPE public.state_type AS ENUM (
    'CREATED',
    'RUNNING',
    'PAUSED',
    'RESTARTED',
    'KILLING',
    'SUCCESS',
    'WARNING',
    'FAILED',
    'KILLED',
    'CANCELLED',
    'QUEUED',
    'RETRYING',
    'RETRIED',
    'SKIPPED',
    'BREAKPOINT'
);


ALTER TYPE public.state_type OWNER TO admin;

--
-- Name: keep_row_number_steady(); Type: FUNCTION; Schema: external; Owner: admin
--

CREATE FUNCTION external.keep_row_number_steady() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    time_interval interval := concat('1',' years ','0',' mons ','0',' days' );
BEGIN
    WHILE ((SELECT count(id) FROM external.external_payload) > 9223372036854775807) OR
       ((SELECT min(created_on) FROM external.external_payload) < (CURRENT_TIMESTAMP - time_interval))
    LOOP
        DELETE FROM external.external_payload
        WHERE created_on = (SELECT min(created_on) FROM external.external_payload);
    END LOOP;
    RETURN NULL;
END;
$$;


ALTER FUNCTION external.keep_row_number_steady() OWNER TO admin;

--
-- Name: fulltext_replace(text, text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.fulltext_replace(text, text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    RETURN TRIM(BOTH $2 FROM array_to_string(ARRAY(SELECT DISTINCT a.a FROM unnest(regexp_split_to_array(COALESCE($1, ''::text), '[^a-zA-Z\d]'::text)) a(a) WHERE (a.a <> ''::text)), $2));


ALTER FUNCTION public.fulltext_replace(text, text) OWNER TO admin;

--
-- Name: fulltext_index(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.fulltext_index(text) RETURNS tsvector
    LANGUAGE sql IMMUTABLE STRICT
    RETURN (to_tsvector('simple'::regconfig, public.fulltext_replace($1, ' '::text)) || to_tsvector('simple'::regconfig, $1));


ALTER FUNCTION public.fulltext_index(text) OWNER TO admin;

--
-- Name: fulltext_search(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.fulltext_search(text) RETURNS tsquery
    LANGUAGE sql IMMUTABLE STRICT
    RETURN CASE WHEN (public.fulltext_replace($1, ''::text) = ''::text) THEN to_tsquery(''::text) ELSE to_tsquery('simple'::regconfig, (public.fulltext_replace($1, ':* & '::text) || ':*'::text)) END;


ALTER FUNCTION public.fulltext_search(text) OWNER TO admin;

--
-- Name: loglevel_fromtext(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.loglevel_fromtext(text) RETURNS public.log_level
    LANGUAGE sql IMMUTABLE
    RETURN ($1)::public.log_level;


ALTER FUNCTION public.loglevel_fromtext(text) OWNER TO admin;

--
-- Name: parse_iso8601_datetime(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.parse_iso8601_datetime(text) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE
    RETURN ($1)::timestamp with time zone;


ALTER FUNCTION public.parse_iso8601_datetime(text) OWNER TO admin;

--
-- Name: parse_iso8601_duration(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.parse_iso8601_duration(text) RETURNS interval
    LANGUAGE sql IMMUTABLE
    RETURN ($1)::interval;


ALTER FUNCTION public.parse_iso8601_duration(text) OWNER TO admin;

--
-- Name: parse_iso8601_timestamp(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.parse_iso8601_timestamp(text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    RETURN EXTRACT(epoch FROM (($1)::timestamp with time zone AT TIME ZONE 'utc'::text));


ALTER FUNCTION public.parse_iso8601_timestamp(text) OWNER TO admin;

--
-- Name: poll_data_update_check(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.poll_data_update_check() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF(NEW.json_data::json ->> 'lastPollTime')::BIGINT < (OLD.json_data::json ->> 'lastPollTime')::BIGINT THEN
        RAISE EXCEPTION 'lastPollTime cannot be set to a lower value';
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.poll_data_update_check() OWNER TO admin;

--
-- Name: state_fromtext(text); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.state_fromtext(text) RETURNS public.state_type
    LANGUAGE sql IMMUTABLE
    RETURN ($1)::public.state_type;


ALTER FUNCTION public.state_fromtext(text) OWNER TO admin;

--
-- Name: update_updated_datetime(); Type: FUNCTION; Schema: public; Owner: admin
--

CREATE FUNCTION public.update_updated_datetime() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated = now();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.update_updated_datetime() OWNER TO admin;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: external_payload; Type: TABLE; Schema: external; Owner: admin
--

CREATE TABLE external.external_payload (
    id text NOT NULL,
    data bytea NOT NULL,
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE ONLY external.external_payload ALTER COLUMN data SET STORAGE EXTERNAL;


ALTER TABLE external.external_payload OWNER TO admin;

--
-- Name: flyway_schema_history; Type: TABLE; Schema: external; Owner: admin
--

CREATE TABLE external.flyway_schema_history (
    installed_rank integer NOT NULL,
    version character varying(50),
    description character varying(200) NOT NULL,
    type character varying(20) NOT NULL,
    script character varying(1000) NOT NULL,
    checksum integer,
    installed_by character varying(100) NOT NULL,
    installed_on timestamp without time zone DEFAULT now() NOT NULL,
    execution_time integer NOT NULL,
    success boolean NOT NULL
);


ALTER TABLE external.flyway_schema_history OWNER TO admin;

--
-- Name: dashboards; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.dashboards (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    id character varying(100) GENERATED ALWAYS AS ((value ->> 'id'::text)) STORED NOT NULL,
    title character varying(250) GENERATED ALWAYS AS ((value ->> 'title'::text)) STORED NOT NULL,
    description text GENERATED ALWAYS AS ((value ->> 'description'::text)) STORED,
    fulltext tsvector GENERATED ALWAYS AS (public.fulltext_index((((value ->> 'title'::text))::character varying)::text)) STORED,
    source_code text NOT NULL,
    created timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


ALTER TABLE public.dashboards OWNER TO admin;

--
-- Name: event_execution; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.event_execution (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    event_handler_name character varying(255) NOT NULL,
    event_name character varying(255) NOT NULL,
    message_id character varying(255) NOT NULL,
    execution_id character varying(255) NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.event_execution OWNER TO admin;

--
-- Name: execution_queued; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.execution_queued (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    date timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'date'::text))) STORED NOT NULL
);


ALTER TABLE public.execution_queued OWNER TO admin;

--
-- Name: execution_running; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.execution_running (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL
);


ALTER TABLE public.execution_running OWNER TO admin;

--
-- Name: executions; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.executions (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    state_current public.state_type GENERATED ALWAYS AS (public.state_fromtext((value #>> '{state,current}'::text[]))) STORED NOT NULL,
    state_duration bigint GENERATED ALWAYS AS (EXTRACT(milliseconds FROM public.parse_iso8601_duration((value #>> '{state,duration}'::text[])))) STORED NOT NULL,
    start_date timestamp without time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value #>> '{state,startDate}'::text[]))) STORED NOT NULL,
    end_date timestamp without time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value #>> '{state,endDate}'::text[]))) STORED,
    fulltext tsvector GENERATED ALWAYS AS (((public.fulltext_index((((value ->> 'namespace'::text))::character varying)::text) || public.fulltext_index((((value ->> 'flowId'::text))::character varying)::text)) || public.fulltext_index((((value ->> 'id'::text))::character varying)::text))) STORED,
    id character varying(150) GENERATED ALWAYS AS ((value ->> 'id'::text)) STORED NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    trigger_execution_id character varying(150) GENERATED ALWAYS AS ((value #>> '{trigger,variables,executionId}'::text[])) STORED,
    kind character varying(32) GENERATED ALWAYS AS ((value ->> 'kind'::text)) STORED
);


ALTER TABLE public.executions OWNER TO admin;

--
-- Name: executordelayed; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.executordelayed (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    date timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'date'::text))) STORED NOT NULL
);


ALTER TABLE public.executordelayed OWNER TO admin;

--
-- Name: executorstate; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.executorstate (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL
);


ALTER TABLE public.executorstate OWNER TO admin;

--
-- Name: flow_topologies; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.flow_topologies (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    source_namespace character varying(150) GENERATED ALWAYS AS ((value #>> '{source,namespace}'::text[])) STORED NOT NULL,
    source_id character varying(150) GENERATED ALWAYS AS ((value #>> '{source,id}'::text[])) STORED NOT NULL,
    relation character varying(100) GENERATED ALWAYS AS ((value ->> 'relation'::text)) STORED NOT NULL,
    destination_namespace character varying(150) GENERATED ALWAYS AS ((value #>> '{destination,namespace}'::text[])) STORED NOT NULL,
    destination_id character varying(150) GENERATED ALWAYS AS ((value #>> '{destination,id}'::text[])) STORED NOT NULL,
    source_tenant_id character varying(250) GENERATED ALWAYS AS ((value #>> '{source,tenantId}'::text[])) STORED,
    destination_tenant_id character varying(250) GENERATED ALWAYS AS ((value #>> '{destination,tenantId}'::text[])) STORED
);


ALTER TABLE public.flow_topologies OWNER TO admin;

--
-- Name: flows; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.flows (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    id character varying(100) GENERATED ALWAYS AS ((value ->> 'id'::text)) STORED NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    revision integer GENERATED ALWAYS AS (((value ->> 'revision'::text))::integer) STORED NOT NULL,
    fulltext tsvector GENERATED ALWAYS AS ((public.fulltext_index((((value ->> 'namespace'::text))::character varying)::text) || public.fulltext_index((((value ->> 'id'::text))::character varying)::text))) STORED,
    source_code text NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED
);


ALTER TABLE public.flows OWNER TO admin;

--
-- Name: flyway_schema_history; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.flyway_schema_history (
    installed_rank integer NOT NULL,
    version character varying(50),
    description character varying(200) NOT NULL,
    type character varying(20) NOT NULL,
    script character varying(1000) NOT NULL,
    checksum integer,
    installed_by character varying(100) NOT NULL,
    installed_on timestamp without time zone DEFAULT now() NOT NULL,
    execution_time integer NOT NULL,
    success boolean NOT NULL
);


ALTER TABLE public.flyway_schema_history OWNER TO admin;

--
-- Name: locks; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.locks (
    lock_id character varying NOT NULL,
    lease_expiration timestamp with time zone NOT NULL
);


ALTER TABLE public.locks OWNER TO admin;

--
-- Name: logs; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.logs (
    key character varying(30) NOT NULL,
    value jsonb NOT NULL,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    task_id character varying(150) GENERATED ALWAYS AS ((value ->> 'taskId'::text)) STORED,
    execution_id character varying(150) GENERATED ALWAYS AS ((value ->> 'executionId'::text)) STORED,
    taskrun_id character varying(150) GENERATED ALWAYS AS ((value ->> 'taskRunId'::text)) STORED,
    attempt_number integer GENERATED ALWAYS AS (((value ->> 'attemptNumber'::text))::integer) STORED,
    trigger_id character varying(150) GENERATED ALWAYS AS ((value ->> 'triggerId'::text)) STORED,
    level public.log_level GENERATED ALWAYS AS (public.loglevel_fromtext((value ->> 'level'::text))) STORED NOT NULL,
    "timestamp" timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'timestamp'::text))) STORED NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    fulltext tsvector GENERATED ALWAYS AS ((((((((public.fulltext_index((((value ->> 'namespace'::text))::character varying)::text) || public.fulltext_index((((value ->> 'flowId'::text))::character varying)::text)) || public.fulltext_index((COALESCE(((value ->> 'taskId'::text))::character varying, ''::character varying))::text)) || public.fulltext_index((COALESCE(((value ->> 'executionId'::text))::character varying, ''::character varying))::text)) || public.fulltext_index((COALESCE(((value ->> 'taskRunId'::text))::character varying, ''::character varying))::text)) || public.fulltext_index((COALESCE(((value ->> 'triggerId'::text))::character varying, ''::character varying))::text)) || public.fulltext_index((COALESCE(((value ->> 'message'::text))::character varying, ''::character varying))::text)) || public.fulltext_index((COALESCE(((value ->> 'thread'::text))::character varying, ''::character varying))::text))) STORED,
    execution_kind character varying(32) GENERATED ALWAYS AS ((value ->> 'executionKind'::text)) STORED
);


ALTER TABLE public.logs OWNER TO admin;

--
-- Name: meta_event_handler; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.meta_event_handler (
    id integer NOT NULL,
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    name character varying(255) NOT NULL,
    event character varying(255) NOT NULL,
    active boolean NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.meta_event_handler OWNER TO admin;

--
-- Name: meta_event_handler_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.meta_event_handler_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.meta_event_handler_id_seq OWNER TO admin;

--
-- Name: meta_event_handler_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.meta_event_handler_id_seq OWNED BY public.meta_event_handler.id;


--
-- Name: meta_task_def; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.meta_task_def (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    name character varying(255) NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.meta_task_def OWNER TO admin;

--
-- Name: meta_workflow_def; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.meta_workflow_def (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    name character varying(255) NOT NULL,
    version integer NOT NULL,
    latest_version integer DEFAULT 0 NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.meta_workflow_def OWNER TO admin;

--
-- Name: metrics; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.metrics (
    key character varying(30) NOT NULL,
    value jsonb NOT NULL,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    task_id character varying(150) GENERATED ALWAYS AS ((value ->> 'taskId'::text)) STORED NOT NULL,
    execution_id character varying(150) GENERATED ALWAYS AS ((value ->> 'executionId'::text)) STORED NOT NULL,
    taskrun_id character varying(150) GENERATED ALWAYS AS ((value ->> 'taskRunId'::text)) STORED NOT NULL,
    metric_name character varying(150) GENERATED ALWAYS AS ((value ->> 'name'::text)) STORED NOT NULL,
    "timestamp" timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'timestamp'::text))) STORED NOT NULL,
    metric_value double precision GENERATED ALWAYS AS (((value ->> 'value'::text))::double precision) STORED NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    execution_kind character varying(32) GENERATED ALWAYS AS ((value ->> 'executionKind'::text)) STORED
);


ALTER TABLE public.metrics OWNER TO admin;

--
-- Name: multipleconditions; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.multipleconditions (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    condition_id character varying(150) GENERATED ALWAYS AS ((value ->> 'conditionId'::text)) STORED NOT NULL,
    start_date timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'start'::text))) STORED NOT NULL,
    end_date timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'end'::text))) STORED NOT NULL,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED
);


ALTER TABLE public.multipleconditions OWNER TO admin;

--
-- Name: poll_data; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.poll_data (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    queue_name character varying(255) NOT NULL,
    domain character varying(255) NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.poll_data OWNER TO admin;

--
-- Name: queue; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.queue (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    queue_name character varying(255) NOT NULL
);


ALTER TABLE public.queue OWNER TO admin;

--
-- Name: queue_message; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.queue_message (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    deliver_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    queue_name character varying(255) NOT NULL,
    message_id character varying(255) NOT NULL,
    priority integer DEFAULT 0,
    popped boolean DEFAULT false,
    offset_time_seconds bigint,
    payload text
);


ALTER TABLE public.queue_message OWNER TO admin;

--
-- Name: queues; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.queues (
    "offset" bigint NOT NULL,
    type public.queue_type NOT NULL,
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    updated timestamp with time zone,
    consumer_indexer boolean DEFAULT false,
    consumer_executor boolean DEFAULT false,
    consumer_worker boolean DEFAULT false,
    consumer_scheduler boolean DEFAULT false,
    consumer_flow_topology boolean DEFAULT false,
    consumer_group character varying(250)
);


ALTER TABLE public.queues OWNER TO admin;

--
-- Name: queues_offset_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.queues_offset_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.queues_offset_seq OWNER TO admin;

--
-- Name: queues_offset_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.queues_offset_seq OWNED BY public.queues."offset";


--
-- Name: service_instance; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.service_instance (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    service_id character varying(36) GENERATED ALWAYS AS ((value ->> 'id'::text)) STORED NOT NULL,
    service_type character varying(36) GENERATED ALWAYS AS ((value ->> 'type'::text)) STORED NOT NULL,
    state character varying(36) GENERATED ALWAYS AS ((value ->> 'state'::text)) STORED NOT NULL,
    created_at timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'createdAt'::text))) STORED NOT NULL,
    updated_at timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'updatedAt'::text))) STORED NOT NULL
);


ALTER TABLE public.service_instance OWNER TO admin;

--
-- Name: settings; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.settings (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL
);


ALTER TABLE public.settings OWNER TO admin;

--
-- Name: sla_monitor; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.sla_monitor (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    execution_id character varying(150) GENERATED ALWAYS AS ((value ->> 'executionId'::text)) STORED NOT NULL,
    sla_id character varying(150) GENERATED ALWAYS AS ((value ->> 'slaId'::text)) STORED NOT NULL,
    deadline timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'deadline'::text))) STORED NOT NULL
);


ALTER TABLE public.sla_monitor OWNER TO admin;

--
-- Name: task; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.task (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    task_id character varying(255) NOT NULL,
    json_data text NOT NULL
);


ALTER TABLE public.task OWNER TO admin;

--
-- Name: task_execution_logs; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.task_execution_logs (
    log_id integer NOT NULL,
    task_id character varying(255) NOT NULL,
    log text NOT NULL,
    created_time timestamp with time zone NOT NULL
);


ALTER TABLE public.task_execution_logs OWNER TO admin;

--
-- Name: task_execution_logs_log_id_seq; Type: SEQUENCE; Schema: public; Owner: admin
--

CREATE SEQUENCE public.task_execution_logs_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.task_execution_logs_log_id_seq OWNER TO admin;

--
-- Name: task_execution_logs_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin
--

ALTER SEQUENCE public.task_execution_logs_log_id_seq OWNED BY public.task_execution_logs.log_id;


--
-- Name: task_in_progress; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.task_in_progress (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    task_def_name character varying(255) NOT NULL,
    task_id character varying(255) NOT NULL,
    workflow_id character varying(255) NOT NULL,
    in_progress_status boolean DEFAULT false NOT NULL
);


ALTER TABLE public.task_in_progress OWNER TO admin;

--
-- Name: task_index; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.task_index (
    task_id character varying(255) NOT NULL,
    task_type text NOT NULL,
    task_def_name text NOT NULL,
    status character varying(32) NOT NULL,
    start_time timestamp with time zone NOT NULL,
    update_time timestamp with time zone NOT NULL,
    workflow_type character varying(128) NOT NULL,
    json_data jsonb NOT NULL
);


ALTER TABLE public.task_index OWNER TO admin;

--
-- Name: task_scheduled; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.task_scheduled (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    workflow_id character varying(255) NOT NULL,
    task_key character varying(255) NOT NULL,
    task_id character varying(255) NOT NULL
);


ALTER TABLE public.task_scheduled OWNER TO admin;

--
-- Name: templates; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.templates (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    deleted boolean GENERATED ALWAYS AS (((value ->> 'deleted'::text))::boolean) STORED NOT NULL,
    id character varying(100) GENERATED ALWAYS AS ((value ->> 'id'::text)) STORED NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    fulltext tsvector GENERATED ALWAYS AS (public.fulltext_index(((public.fulltext_replace((((value ->> 'namespace'::text))::character varying)::text, ' '::text) || ' '::text) || public.fulltext_replace((((value ->> 'id'::text))::character varying)::text, ' '::text)))) STORED,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED
);


ALTER TABLE public.templates OWNER TO admin;

--
-- Name: triggers; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.triggers (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    namespace character varying(150) GENERATED ALWAYS AS ((value ->> 'namespace'::text)) STORED NOT NULL,
    flow_id character varying(150) GENERATED ALWAYS AS ((value ->> 'flowId'::text)) STORED NOT NULL,
    trigger_id character varying(150) GENERATED ALWAYS AS ((value ->> 'triggerId'::text)) STORED NOT NULL,
    execution_id character varying(150) GENERATED ALWAYS AS ((value ->> 'executionId'::text)) STORED,
    fulltext tsvector GENERATED ALWAYS AS ((((public.fulltext_index((((value ->> 'namespace'::text))::character varying)::text) || public.fulltext_index((((value ->> 'flowId'::text))::character varying)::text)) || public.fulltext_index((((value ->> 'triggerId'::text))::character varying)::text)) || public.fulltext_index((COALESCE(((value ->> 'executionId'::text))::character varying, ''::character varying))::text))) STORED,
    tenant_id character varying(250) GENERATED ALWAYS AS ((value ->> 'tenantId'::text)) STORED,
    next_execution_date timestamp with time zone GENERATED ALWAYS AS (public.parse_iso8601_datetime((value ->> 'nextExecutionDate'::text))) STORED,
    worker_id character varying(250) GENERATED ALWAYS AS ((value ->> 'workerId'::text)) STORED
);


ALTER TABLE public.triggers OWNER TO admin;

--
-- Name: worker_job_running; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.worker_job_running (
    key character varying(250) NOT NULL,
    value jsonb NOT NULL,
    worker_uuid character varying(36) GENERATED ALWAYS AS (((value -> 'workerInstance'::text) ->> 'workerUuid'::text)) STORED NOT NULL
);


ALTER TABLE public.worker_job_running OWNER TO admin;

--
-- Name: workflow; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.workflow (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    workflow_id character varying(255) NOT NULL,
    correlation_id character varying(255),
    json_data text NOT NULL
);


ALTER TABLE public.workflow OWNER TO admin;

--
-- Name: workflow_def_to_workflow; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.workflow_def_to_workflow (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    workflow_def character varying(255) NOT NULL,
    date_str character varying(60) NOT NULL,
    workflow_id character varying(255) NOT NULL
);


ALTER TABLE public.workflow_def_to_workflow OWNER TO admin;

--
-- Name: workflow_index; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.workflow_index (
    workflow_id character varying(255) NOT NULL,
    correlation_id character varying(128),
    workflow_type character varying(128) NOT NULL,
    start_time timestamp with time zone NOT NULL,
    status character varying(32) NOT NULL,
    json_data jsonb NOT NULL,
    update_time timestamp with time zone DEFAULT '1970-01-01 00:00:00+00'::timestamp with time zone NOT NULL
);


ALTER TABLE public.workflow_index OWNER TO admin;

--
-- Name: workflow_pending; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.workflow_pending (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    workflow_type character varying(255) NOT NULL,
    workflow_id character varying(255) NOT NULL
);


ALTER TABLE public.workflow_pending OWNER TO admin;

--
-- Name: workflow_to_task; Type: TABLE; Schema: public; Owner: admin
--

CREATE TABLE public.workflow_to_task (
    created_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_on timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    workflow_id character varying(255) NOT NULL,
    task_id character varying(255) NOT NULL
);


ALTER TABLE public.workflow_to_task OWNER TO admin;

--
-- Name: meta_event_handler id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.meta_event_handler ALTER COLUMN id SET DEFAULT nextval('public.meta_event_handler_id_seq'::regclass);


--
-- Name: queues offset; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.queues ALTER COLUMN "offset" SET DEFAULT nextval('public.queues_offset_seq'::regclass);


--
-- Name: task_execution_logs log_id; Type: DEFAULT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task_execution_logs ALTER COLUMN log_id SET DEFAULT nextval('public.task_execution_logs_log_id_seq'::regclass);


--
-- Name: external_payload external_payload_pkey; Type: CONSTRAINT; Schema: external; Owner: admin
--

ALTER TABLE ONLY external.external_payload
    ADD CONSTRAINT external_payload_pkey PRIMARY KEY (id);


--
-- Name: flyway_schema_history flyway_schema_history_pk; Type: CONSTRAINT; Schema: external; Owner: admin
--

ALTER TABLE ONLY external.flyway_schema_history
    ADD CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank);


--
-- Name: dashboards dashboards_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.dashboards
    ADD CONSTRAINT dashboards_pkey PRIMARY KEY (key);


--
-- Name: event_execution event_execution_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.event_execution
    ADD CONSTRAINT event_execution_pkey PRIMARY KEY (event_handler_name, event_name, execution_id);


--
-- Name: execution_queued execution_queued_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.execution_queued
    ADD CONSTRAINT execution_queued_pkey PRIMARY KEY (key);


--
-- Name: execution_running execution_running_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.execution_running
    ADD CONSTRAINT execution_running_pkey PRIMARY KEY (key);


--
-- Name: executions executions_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.executions
    ADD CONSTRAINT executions_pkey PRIMARY KEY (key);


--
-- Name: executordelayed executordelayed_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.executordelayed
    ADD CONSTRAINT executordelayed_pkey PRIMARY KEY (key);


--
-- Name: executorstate executorstate_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.executorstate
    ADD CONSTRAINT executorstate_pkey PRIMARY KEY (key);


--
-- Name: flow_topologies flow_topologies_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.flow_topologies
    ADD CONSTRAINT flow_topologies_pkey PRIMARY KEY (key);


--
-- Name: flows flows_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.flows
    ADD CONSTRAINT flows_pkey PRIMARY KEY (key);


--
-- Name: flyway_schema_history flyway_schema_history_pk; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.flyway_schema_history
    ADD CONSTRAINT flyway_schema_history_pk PRIMARY KEY (installed_rank);


--
-- Name: locks locks_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.locks
    ADD CONSTRAINT locks_pkey PRIMARY KEY (lock_id);


--
-- Name: logs logs_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (key);


--
-- Name: meta_event_handler meta_event_handler_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.meta_event_handler
    ADD CONSTRAINT meta_event_handler_pkey PRIMARY KEY (id);


--
-- Name: meta_task_def meta_task_def_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.meta_task_def
    ADD CONSTRAINT meta_task_def_pkey PRIMARY KEY (name);


--
-- Name: meta_workflow_def meta_workflow_def_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.meta_workflow_def
    ADD CONSTRAINT meta_workflow_def_pkey PRIMARY KEY (name, version);


--
-- Name: metrics metrics_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.metrics
    ADD CONSTRAINT metrics_pkey PRIMARY KEY (key);


--
-- Name: multipleconditions multipleconditions_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.multipleconditions
    ADD CONSTRAINT multipleconditions_pkey PRIMARY KEY (key);


--
-- Name: poll_data poll_data_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.poll_data
    ADD CONSTRAINT poll_data_pkey PRIMARY KEY (queue_name, domain);


--
-- Name: queue_message queue_message_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.queue_message
    ADD CONSTRAINT queue_message_pkey PRIMARY KEY (queue_name, message_id);


--
-- Name: queue queue_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.queue
    ADD CONSTRAINT queue_pkey PRIMARY KEY (queue_name);


--
-- Name: service_instance service_instance_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.service_instance
    ADD CONSTRAINT service_instance_pkey PRIMARY KEY (key);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (key);


--
-- Name: sla_monitor sla_monitor_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.sla_monitor
    ADD CONSTRAINT sla_monitor_pkey PRIMARY KEY (key);


--
-- Name: task_execution_logs task_execution_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task_execution_logs
    ADD CONSTRAINT task_execution_logs_pkey PRIMARY KEY (log_id);


--
-- Name: task_in_progress task_in_progress_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task_in_progress
    ADD CONSTRAINT task_in_progress_pkey PRIMARY KEY (task_def_name, task_id);


--
-- Name: task_index task_index_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task_index
    ADD CONSTRAINT task_index_pkey PRIMARY KEY (task_id);


--
-- Name: task task_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT task_pkey PRIMARY KEY (task_id);


--
-- Name: task_scheduled task_scheduled_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.task_scheduled
    ADD CONSTRAINT task_scheduled_pkey PRIMARY KEY (workflow_id, task_key);


--
-- Name: templates templates_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.templates
    ADD CONSTRAINT templates_pkey PRIMARY KEY (key);


--
-- Name: triggers triggers_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.triggers
    ADD CONSTRAINT triggers_pkey PRIMARY KEY (key);


--
-- Name: worker_job_running worker_job_running_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.worker_job_running
    ADD CONSTRAINT worker_job_running_pkey PRIMARY KEY (key);


--
-- Name: workflow_def_to_workflow workflow_def_to_workflow_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.workflow_def_to_workflow
    ADD CONSTRAINT workflow_def_to_workflow_pkey PRIMARY KEY (workflow_def, date_str, workflow_id);


--
-- Name: workflow_index workflow_index_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.workflow_index
    ADD CONSTRAINT workflow_index_pkey PRIMARY KEY (workflow_id);


--
-- Name: workflow_pending workflow_pending_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.workflow_pending
    ADD CONSTRAINT workflow_pending_pkey PRIMARY KEY (workflow_type, workflow_id);


--
-- Name: workflow workflow_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.workflow
    ADD CONSTRAINT workflow_pkey PRIMARY KEY (workflow_id);


--
-- Name: workflow_to_task workflow_to_task_pkey; Type: CONSTRAINT; Schema: public; Owner: admin
--

ALTER TABLE ONLY public.workflow_to_task
    ADD CONSTRAINT workflow_to_task_pkey PRIMARY KEY (workflow_id, task_id);


--
-- Name: flyway_schema_history_s_idx; Type: INDEX; Schema: external; Owner: admin
--

CREATE INDEX flyway_schema_history_s_idx ON external.flyway_schema_history USING btree (success);


--
-- Name: combo_queue_message; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX combo_queue_message ON public.queue_message USING btree (queue_name, priority DESC, popped, deliver_on, created_on);


--
-- Name: dashboards_fulltext; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX dashboards_fulltext ON public.dashboards USING gin (fulltext);


--
-- Name: dashboards_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX dashboards_id ON public.dashboards USING btree (id, deleted, tenant_id);


--
-- Name: dashboards_tenant; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX dashboards_tenant ON public.dashboards USING btree (deleted, tenant_id);


--
-- Name: event_handler_event_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX event_handler_event_index ON public.meta_event_handler USING btree (event);


--
-- Name: event_handler_name_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX event_handler_name_index ON public.meta_event_handler USING btree (name);


--
-- Name: execution_queued__flow_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX execution_queued__flow_date ON public.execution_queued USING btree (tenant_id, namespace, flow_id, date);


--
-- Name: execution_running__flow; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX execution_running__flow ON public.execution_running USING btree (tenant_id, namespace, flow_id);


--
-- Name: executions_end_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_end_date ON public.executions USING btree (deleted, tenant_id, end_date);


--
-- Name: executions_flow_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_flow_id ON public.executions USING btree (deleted, tenant_id, flow_id);


--
-- Name: executions_fulltext; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_fulltext ON public.executions USING gin (fulltext);


--
-- Name: executions_labels; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_labels ON public.executions USING gin (((value -> 'labels'::text)));


--
-- Name: executions_namespace; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_namespace ON public.executions USING btree (deleted, tenant_id, namespace);


--
-- Name: executions_start_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_start_date ON public.executions USING btree (deleted, tenant_id, start_date);


--
-- Name: executions_state_current; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_state_current ON public.executions USING btree (deleted, tenant_id, state_current);


--
-- Name: executions_state_duration; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_state_duration ON public.executions USING btree (deleted, tenant_id, state_duration);


--
-- Name: executions_trigger_execution_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executions_trigger_execution_id ON public.executions USING btree (deleted, tenant_id, trigger_execution_id);


--
-- Name: executordelayed_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX executordelayed_date ON public.executordelayed USING btree (date);


--
-- Name: flow_topologies_destination; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flow_topologies_destination ON public.flow_topologies USING btree (destination_tenant_id, destination_namespace, destination_id);


--
-- Name: flow_topologies_destination__source; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flow_topologies_destination__source ON public.flow_topologies USING btree (destination_tenant_id, destination_namespace, destination_id, source_tenant_id, source_namespace, source_id);


--
-- Name: flows_fulltext; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flows_fulltext ON public.flows USING gin (fulltext);


--
-- Name: flows_labels; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flows_labels ON public.flows USING gin (((value -> 'labels'::text)));


--
-- Name: flows_namespace; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flows_namespace ON public.flows USING btree (deleted, tenant_id, namespace);


--
-- Name: flows_namespace__id__revision; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flows_namespace__id__revision ON public.flows USING btree (deleted, tenant_id, namespace, id, revision);


--
-- Name: flows_source_code; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flows_source_code ON public.flows USING gin (public.fulltext_index(source_code));


--
-- Name: flyway_schema_history_s_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX flyway_schema_history_s_idx ON public.flyway_schema_history USING btree (success);


--
-- Name: ix_service_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE UNIQUE INDEX ix_service_id ON public.service_instance USING btree (service_id);


--
-- Name: ix_service_instance_state; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX ix_service_instance_state ON public.service_instance USING btree (state);


--
-- Name: ix_service_instance_type_created_at_updated_at; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX ix_service_instance_type_created_at_updated_at ON public.service_instance USING btree (service_type, created_at, updated_at);


--
-- Name: logs_execution_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX logs_execution_id ON public.logs USING btree (deleted, execution_id);


--
-- Name: logs_execution_id__task_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX logs_execution_id__task_id ON public.logs USING btree (deleted, execution_id, task_id);


--
-- Name: logs_execution_id__taskrun_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX logs_execution_id__taskrun_id ON public.logs USING btree (deleted, execution_id, taskrun_id);


--
-- Name: logs_namespace_flow; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX logs_namespace_flow ON public.logs USING btree (deleted, tenant_id, "timestamp", level, namespace, flow_id);


--
-- Name: logs_timestamp; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX logs_timestamp ON public.logs USING btree ("timestamp");


--
-- Name: metrics_execution_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX metrics_execution_id ON public.metrics USING btree (deleted, execution_id);


--
-- Name: metrics_flow_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX metrics_flow_id ON public.metrics USING btree (deleted, tenant_id, namespace, flow_id);


--
-- Name: metrics_timestamp; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX metrics_timestamp ON public.metrics USING btree (deleted, tenant_id, "timestamp");


--
-- Name: multipleconditions_namespace__flow_id__condition_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX multipleconditions_namespace__flow_id__condition_id ON public.multipleconditions USING btree (tenant_id, namespace, flow_id, condition_id);


--
-- Name: multipleconditions_start_date__end_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX multipleconditions_start_date__end_date ON public.multipleconditions USING btree (tenant_id, start_date, end_date);


--
-- Name: poll_data_queue_name_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX poll_data_queue_name_idx ON public.poll_data USING btree (queue_name);


--
-- Name: queues_offset; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_offset ON public.queues USING hash ("offset");


--
-- Name: queues_type__consumer_executor; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__consumer_executor ON public.queues USING btree (type, consumer_executor, "offset") WHERE (consumer_executor = false);


--
-- Name: queues_type__consumer_flow_topology; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__consumer_flow_topology ON public.queues USING btree (type, consumer_flow_topology, "offset") WHERE (consumer_flow_topology = false);


--
-- Name: queues_type__consumer_indexer; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__consumer_indexer ON public.queues USING btree (type, consumer_indexer, "offset") WHERE (consumer_indexer = false);


--
-- Name: queues_type__consumer_scheduler; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__consumer_scheduler ON public.queues USING btree (type, consumer_scheduler, "offset") WHERE (consumer_scheduler = false);


--
-- Name: queues_type__consumer_worker; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__consumer_worker ON public.queues USING btree (type, consumer_worker, "offset") WHERE (consumer_worker = false);


--
-- Name: queues_type__key; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__key ON public.queues USING btree (type, key);


--
-- Name: queues_type__offset; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_type__offset ON public.queues USING btree (type, "offset");


--
-- Name: queues_updated; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX queues_updated ON public.queues USING btree (updated);


--
-- Name: sla_monitor__deadline; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX sla_monitor__deadline ON public.sla_monitor USING btree (deadline);


--
-- Name: sla_monitor__execution_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX sla_monitor__execution_id ON public.sla_monitor USING btree (execution_id);


--
-- Name: task_execution_logs_task_id_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_execution_logs_task_id_idx ON public.task_execution_logs USING btree (task_id);


--
-- Name: task_index_json_data_fulltext_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_json_data_fulltext_idx ON public.task_index USING gin (jsonb_to_tsvector('english'::regconfig, json_data, '["all"]'::jsonb));


--
-- Name: task_index_json_data_gin_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_json_data_gin_idx ON public.task_index USING gin (json_data jsonb_path_ops);


--
-- Name: task_index_status_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_status_idx ON public.task_index USING btree (status);


--
-- Name: task_index_task_def_name_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_task_def_name_idx ON public.task_index USING btree (task_def_name);


--
-- Name: task_index_task_id_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_task_id_idx ON public.task_index USING btree (task_id);


--
-- Name: task_index_task_type_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_task_type_idx ON public.task_index USING btree (task_type);


--
-- Name: task_index_update_time_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_update_time_idx ON public.task_index USING btree (update_time);


--
-- Name: task_index_workflow_type_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX task_index_workflow_type_idx ON public.task_index USING btree (workflow_type);


--
-- Name: templates_fulltext; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX templates_fulltext ON public.templates USING gin (fulltext);


--
-- Name: templates_namespace; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX templates_namespace ON public.templates USING btree (deleted, tenant_id, namespace);


--
-- Name: templates_namespace__id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX templates_namespace__id ON public.templates USING btree (deleted, tenant_id, namespace, id);


--
-- Name: triggers__tenant; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX triggers__tenant ON public.triggers USING btree (tenant_id);


--
-- Name: triggers_execution_id; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX triggers_execution_id ON public.triggers USING btree (execution_id);


--
-- Name: triggers_next_execution_date; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX triggers_next_execution_date ON public.triggers USING btree (next_execution_date);


--
-- Name: worker_job_running_worker_uuid; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX worker_job_running_worker_uuid ON public.worker_job_running USING btree (worker_uuid);


--
-- Name: workflow_corr_id_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_corr_id_index ON public.workflow USING btree (correlation_id);


--
-- Name: workflow_def_name_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_def_name_index ON public.meta_workflow_def USING btree (name);


--
-- Name: workflow_id_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_id_index ON public.workflow_to_task USING btree (workflow_id);


--
-- Name: workflow_index_correlation_id_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_correlation_id_idx ON public.workflow_index USING btree (correlation_id);


--
-- Name: workflow_index_json_data_gin_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_json_data_gin_idx ON public.workflow_index USING gin (json_data jsonb_path_ops);


--
-- Name: workflow_index_json_data_json_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_json_data_json_idx ON public.workflow_index USING gin (jsonb_to_tsvector('english'::regconfig, json_data, '["all"]'::jsonb));


--
-- Name: workflow_index_start_time_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_start_time_idx ON public.workflow_index USING btree (start_time);


--
-- Name: workflow_index_status_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_status_idx ON public.workflow_index USING btree (status);


--
-- Name: workflow_index_workflow_type_idx; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_index_workflow_type_idx ON public.workflow_index USING btree (workflow_type);


--
-- Name: workflow_type_index; Type: INDEX; Schema: public; Owner: admin
--

CREATE INDEX workflow_type_index ON public.workflow_pending USING btree (workflow_type);


--
-- Name: external_payload tr_keep_row_number_steady; Type: TRIGGER; Schema: external; Owner: admin
--

CREATE TRIGGER tr_keep_row_number_steady AFTER INSERT ON external.external_payload FOR EACH ROW EXECUTE FUNCTION external.keep_row_number_steady();


--
-- Name: dashboards dashboard_updated; Type: TRIGGER; Schema: public; Owner: admin
--

CREATE TRIGGER dashboard_updated BEFORE UPDATE ON public.dashboards FOR EACH ROW EXECUTE FUNCTION public.update_updated_datetime();


--
-- Name: poll_data poll_data_update_check_trigger; Type: TRIGGER; Schema: public; Owner: admin
--

CREATE TRIGGER poll_data_update_check_trigger BEFORE UPDATE ON public.poll_data FOR EACH ROW EXECUTE FUNCTION public.poll_data_update_check();


--
-- Name: queues queues_updated; Type: TRIGGER; Schema: public; Owner: admin
--

CREATE TRIGGER queues_updated BEFORE UPDATE ON public.queues FOR EACH ROW EXECUTE FUNCTION public.update_updated_datetime();


--
-- PostgreSQL database dump complete
--

