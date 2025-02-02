CREATE SCHEMA "JIRA";

CREATE OR REPLACE FILE FORMAT gzip_json
    TYPE = 'JSON'
    STRIP_NULL_VALUES = TRUE
    COMPRESSION = GZIP
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE read_json
    file_format = gzip_json
    URL = 's3://<bucket>/jira/<date>/'
    CREDENTIALS = (AWS_KEY_ID = '<key>' AWS_SECRET_KEY = '<secret_key>');

-- -- -- -- -- -- ISSUES
CREATE TABLE ISSUES (
    id VARCHAR(8),
    key VARCHAR(10),
    fields OBJECT,
    expand VARCHAR(256),
    renderedFields OBJECT,
    self VARCHAR(256),
    dateCreated TIMESTAMP_NTZ(9),
    dateAccessed TIMESTAMP_NTZ(9)
);

CREATE TEMPORARY TABLE ISSUES_TEMP (tmp VARIANT);

COPY INTO ISSUES_TEMP FROM @read_json/issues.gz;

INSERT ALL INTO ISSUES
    SELECT
        tmp:id::string id,
        tmp:key::string key,
        tmp:fields fields,
        tmp:expand::string expand,
        tmp:renderedFields renderedFields,
        tmp:self::string self,
        TO_TIMESTAMP_NTZ(convert_timezone('UTC', to_timestamp_tz(tmp:fields:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'))) dateCreated,
        TO_TIMESTAMP_NTZ(tmp:api_dateAccessed) dateAccessed
    FROM ISSUES_TEMP;

SELECT COUNT(*) FROM ISSUES;

-- -- -- -- -- -- CHANGELOGS

CREATE OR REPLACE TABLE CHANGELOGS (
    id VARCHAR(8) NOT NULL,
    key VARCHAR(10) NOT NULL,
    userId VARCHAR(128) NOT NULL,
    changelogItem OBJECT NOT NULL,
    YEAR VARCHAR(4) NOT NULL,
    QUARTER VARCHAR(1) NOT NULL,
    MONTH VARCHAR(2) NOT NULL,
    WEEKOFYEAR VARCHAR(2) NOT NULL,
    dateCreated TIMESTAMP_NTZ(9) NOT NULL,
    dateAccessed TIMESTAMP_NTZ(9) NOT NULL
);

CREATE TEMPORARY TABLE CHANGELOG_TEMP (tmp VARIANT);

COPY INTO CHANGELOG_TEMP FROM @read_json/changelogs.gz;

INSERT ALL INTO CHANGELOGS
  SELECT
      tmp:id::string id,
      tmp:key::string key,
      changelog.value:author:key::string userId,
      item.value changelogItem,
      YEAR(convert_timezone('UTC', to_timestamp_tz(changelog.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))::string year,
      QUARTER(convert_timezone('UTC', to_timestamp_tz(changelog.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))::string quarter,
      MONTH(convert_timezone('UTC', to_timestamp_tz(changelog.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))::string month,
      WEEKOFYEAR(convert_timezone('UTC', to_timestamp_tz(changelog.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM')))::string weekOfYear,
      TO_TIMESTAMP_NTZ(convert_timezone('UTC', to_timestamp_tz(changelog.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'))) created,
      TO_TIMESTAMP_NTZ(tmp:api_dateAccessed) dateAccessed
  FROM
      CHANGELOG_TEMP,
      LATERAL FLATTEN(tmp:changelog) changelog,
      LATERAL FLATTEN(changelog.value:items) item
  ORDER BY created DESC;

SELECT count(distinct key) FROM CHANGELOGS;

-- -- -- -- -- -- COMMENTS

CREATE OR REPLACE TABLE COMMENTS (
    id VARCHAR(8),
    key VARCHAR(10),
    comment OBJECT,
    created TIMESTAMP_NTZ(9),
    updated TIMESTAMP_NTZ(9),
    accessed TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TEMPORARY TABLE COMMENTS_TEMP (tmp VARIANT);

COPY INTO COMMENTS_TEMP FROM @read_json/comments.gz;

INSERT ALL INTO COMMENTS
    SELECT
        tmp:id::string id,
        tmp:key::string key,
        comment.value,
        TO_TIMESTAMP_NTZ(convert_timezone('UTC', to_timestamp_tz(comment.value:created::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'))) created,
        TO_TIMESTAMP_NTZ(convert_timezone('UTC', to_timestamp_tz(comment.value:updated::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF TZHTZM'))) updated,
        TO_TIMESTAMP_NTZ(tmp:api_dateAccessed) accessed
    FROM
        COMMENTS_TEMP,
        LATERAL FLATTEN(tmp:changelog) comment;

SELECT * FROM COMMENTS LIMIT 100;


-- -- -- -- -- -- ISSUE LINKS
CREATE OR REPLACE TABLE ISSUE_LINKS (
    relationType VARCHAR(64),
    idFrom VARCHAR(8),
    keyFrom VARCHAR(10),
    linkType VARCHAR(64),
    relation VARCHAR(64),
    idTo VARCHAR(8),
    keyTo VARCHAR(10)
);

INSERT ALL INTO ISSUE_LINKS
  SELECT
      links.value:type:name::string "RelationType",
      ji.id "idFrom",
      ji.key "keyFrom",
      IFF (links.value:inwardIssue IS NOT NULL, 'inward', 'outward') "LinkType",
      IFF ("LinkType" = 'inward', links.value:type:inward, links.value:type:outward)::string "Relation",
      IFF ("LinkType" = 'inward', links.value:inwardIssue:id, links.value:outwardIssue:id)::string "idTo",
      IFF ("LinkType" = 'inward', links.value:inwardIssue:key, links.value:outwardIssue:key)::string "keyTo"
  FROM
      "ISSUES" ji,
      LATERAL FLATTEN(ji.fields:issuelinks) links
  WHERE "keyTo" LIKE 'MAB-%';


-- -- -- -- -- -- USERS
CREATE TABLE USERS (
  accountId VARCHAR(128),
  active BOOLEAN,
  accountType VARCHAR(32),
  displayName VARCHAR(62),
  email VARCHAR(128),
  userKey VARCHAR(128),
  other OBJECT
);

CREATE OR REPLACE TEMPORARY TABLE USERS_TEMP (tmp VARIANT);
COPY INTO USERS_TEMP FROM @read_json/users.gz;

INSERT ALL INTO USERS
  SELECT
      tmp:accountId::string accountId,
      tmp:active active,
      tmp:accountType::string accountType,
      tmp:displayName::string displayName,
      tmp:emailAddress::string email,
      tmp:key::string userKey,
      tmp other
  FROM USERS_TEMP;

SELECT * FROM USERS;

-- -- -- -- -- -- TIMELINES
CREATE TABLE TIMELINES (
    KEY       VARCHAR(32),
    STATUS    VARCHAR(256),
    ASSIGNEE  VARCHAR(128),
    DATEFROM  TIMESTAMP_NTZ(9),
    DATETO    TIMESTAMP_NTZ(9),
    TIMEDELTA NUMBER(38, 0)
);

-- -- -- -- -- -- STATUSES
CREATE TABLE STATUSES(id VARCHAR(64));
INSERT ALL INTO STATUSES SELECT DISTINCT STATUS FROM TIMELINES;
