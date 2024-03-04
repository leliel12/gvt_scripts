import pathlib
import tempfile
import datetime
import re
import ast

import attrs

import dateutil.parser as dateparser

import peewee as pw

from ..utils import TEMP_DIR


# =============================================================================
# DATETIME HELPERS MOCELS
# =============================================================================


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


class DateableABC(pw.Model):

    created_at = pw.DateTimeField(default=_utc_now)
    updated_at = pw.DateTimeField(default=_utc_now)


# =============================================================================
# MIXINS
# =============================================================================


class MetaDataDirectoryMixin(DateableABC):

    date_str = pw.CharField(unique=True)
    path_str = pw.CharField()

    @property
    def path(self):
        return pathlib.Path(self.path_str)


class DBFRecordMixin(DateableABC):

    md_directory = pw.ForeignKeyField(MetaDataDirectoryMixin)

    batch = pw.CharField()
    tarsize = pw.IntegerField()
    satellite = pw.CharField()
    sensorid = pw.CharField()
    acquisitio = pw.DateField()
    cloudperce = pw.IntegerField()
    orbitid = pw.IntegerField()
    scenepath = pw.IntegerField()
    scenerow = pw.IntegerField()


class PRJMixin(DateableABC):

    md_directory = pw.ForeignKeyField(MetaDataDirectoryMixin)

    schema = pw.CharField()
    type = pw.CharField()
    name = pw.CharField()

    datum_type = pw.CharField()
    datum_name = pw.CharField()
    datum_ellipsoid_name = pw.CharField()
    datum_ellipsoid_semi_major_axis = pw.IntegerField()
    datum_ellipsoid_inverse_flattening = pw.FloatField()
    datum_id_authority = pw.CharField()
    datum_id_code = pw.IntegerField()

    coordinate_system_subtype = pw.CharField()


class CoordinateSystemAxisEntryMixin(DateableABC):

    prj = pw.ForeignKeyField(PRJMixin)

    name = pw.CharField()
    abbreviation = pw.CharField()
    direction = pw.CharField()
    unit_type = pw.CharField()
    unit_name = pw.CharField()
    unit_conversion_factor = pw.FloatField()


class JGWMixin(DateableABC):

    md_directory = pw.ForeignKeyField(MetaDataDirectoryMixin)

    path = pw.CharField()
    jpg = pw.CharField()
    scale_x = pw.FloatField()
    rotation_y = pw.FloatField()
    rotation_x = pw.FloatField()
    scale_y = pw.FloatField()
    upper_left_x = pw.FloatField()
    upper_left_y = pw.FloatField()


# =============================================================================
# DAL
# =============================================================================


def _model_field(mixin):

    model_name = mixin.__name__.removesuffix("Mixin")

    def _make(db):
        model = type(model_name, (db.BaseModel, mixin), {})
        return model

    default = attrs.Factory(_make, takes_self=True)
    return attrs.field(init=False, repr=False, default=default)


@attrs.define(frozen=True)
class Database:

    db: pw.Database = attrs.field()
    BaseModel: pw.Model = attrs.field(init=False, repr=False)

    @BaseModel.default
    def _BaseModel_default(self):
        class BaseModel(pw.Model):

            class Meta:
                database = self.db

        return BaseModel

    MetaDataDirectory: pw.Model = _model_field(MetaDataDirectoryMixin)
    DBFRecord: pw.Model = _model_field(DBFRecordMixin)
    PRJ = _model_field(PRJMixin)
    CoordinateSystemAxisEntry = _model_field(CoordinateSystemAxisEntryMixin)
    JGW = _model_field(JGWMixin)

    @classmethod
    def from_url(cls, url):
        """Alternative constructor."""
        if url is None:
            _, url = tempfile.mkstemp(suffix=".sqlite3", dir=TEMP_DIR.name)

        db = pw.SqliteDatabase(url)
        instance = cls(db=db)

        return instance

    def __attrs_post_init__(self):
        self.db.connect()
        self.db.create_tables(self.models)

    @property
    def models(self):
        """This property returns a list of models by filtering the \
        attributes."""

        def flt(aname, avalue):
            return avalue is not self.BaseModel and isinstance(
                avalue, pw.ModelBase
            )

        return list(attrs.asdict(self, filter=flt).values())

    def get_or_create_mdir(self, mdir_path_or_reg):
        if isinstance(mdir_path_or_reg, self.MetaDataDirectory):
            return mdir_path_or_reg

        mdir = pathlib.Path(mdir_path_or_reg)
        reg = self.MetaDataDirectory.get_or_create(
            date_str=mdir.parent.name,
            defaults={"path_str": str(mdir)},
        )
        return reg[0]

    def store_dbfreg(
        self,
        mdir,
        *,
        batch,
        tarsize,
        satellite,
        sensorid,
        acquisitio,
        cloudperce,
        orbitid,
        scenepath,
        scenerow,
    ):
        mdir_reg = self.get_or_create_mdir(mdir_path_or_reg=mdir)

        reg = self.DBFRecord(
            md_directory=mdir_reg,
            batch=batch,
            tarsize=int(tarsize),
            satellite=satellite,
            sensorid=sensorid,
            acquisitio=acquisitio,
            cloudperce=int(cloudperce),
            orbitid=int(orbitid),
            scenepath=int(scenepath),
            scenerow=int(scenerow),
        )
        reg.save()
        return reg

    def _store_coord_axis(self, prj, *, name, abbreviation, direction, unit):
        reg = self.CoordinateSystemAxisEntry(
            prj=prj,
            name=name,
            abbreviation=abbreviation,
            direction=direction,
            unit_type=unit["type"],
            unit_name=unit["name"],
            unit_conversion_factor=unit["conversion_factor"],
        )
        reg.save()
        return reg

    def store_prj(
        self,
        mdir_path_or_reg,
        *,
        schema,
        type,
        name,
        datum,
        coordinate_system,
    ):

        mdir_reg = self.get_or_create_mdir(mdir_path_or_reg=mdir_path_or_reg)

        prj_reg = self.PRJ(
            md_directory=mdir_reg,
            schema=schema,
            type=type,
            name=name,
            datum_type=datum["type"],
            datum_name=datum["name"],
            datum_ellipsoid_name=datum["ellipsoid"]["name"],
            datum_ellipsoid_semi_major_axis=(
                datum["ellipsoid"]["semi_major_axis"]
            ),
            datum_ellipsoid_inverse_flattening=(
                datum["ellipsoid"]["inverse_flattening"]
            ),
            datum_id_authority=datum["id"]["authority"],
            datum_id_code=datum["id"]["code"],
            coordinate_system_subtype=coordinate_system["subtype"],
        )

        prj_reg.save()

        for axis in coordinate_system["axis"]:
            self._store_coord_axis(prj=prj_reg, **axis)

        return prj_reg

    def store_jgw(
        self,
        mdir_path_or_reg,
        *,
        path,
        scale_x,
        rotation_y,
        rotation_x,
        scale_y,
        upper_left_x,
        upper_left_y,
        jpg_path,
    ):

        mdir_reg = self.get_or_create_mdir(mdir_path_or_reg=mdir_path_or_reg)

        jgw_reg = self.JGW(
            md_directory=mdir_reg,
            path=path,
            scale_x=scale_x,
            rotation_y=rotation_y,
            rotation_x=rotation_x,
            scale_y=scale_y,
            upper_left_x=upper_left_x,
            upper_left_y=upper_left_y,
            jpg=jpg_path,
        )

        jgw_reg.save()

        return jgw_reg

    # QUERY ===================================================================

    SIMPLE_QUERY_PATTERN = re.compile(
        r"(?P<arg>\w+)\s*"
        "(?P<op>!=|=|<=|<|>=|>|in|not in)\s*"
        "(?P<value>.+)"
    )

    @attrs.define(frozen=True)
    class _Operation:
        arg: str = attrs.field(
            validator=attrs.validators.matches_re(r"^[a-zA-Z][a-zA-Z0-9_]*$")
        )
        operation: str = attrs.field(
            validator=attrs.validators.in_(
                ["=", "!=", ">", ">=", "<", "<=", "in", "not in"]
            )
        )
        value = attrs.field()

    def parse_simple_query(self, query_str):

        operations = []
        for squery_str in query_str.split("&"):
            match = self.SIMPLE_QUERY_PATTERN.match(squery_str.strip())

            if not match:
                raise ValueError(
                    f"Invalid query: {query_str!r}. Hint {squery_str!r}"
                )

            groups = match.groupdict()

            arg = groups["arg"].strip()
            operation = groups["op"].strip()
            value_str = groups["value"].strip()

            if operation in ("in", "not in"):
                value = ast.literal_eval(value_str)
            else:
                value = value_str
            try:
                operations.append(self._Operation(arg, operation, value))
            except Exception as err:
                raise ValueError(
                    f"Invalid query: {query_str!r}. Hint {squery_str!r}"
                ) from err

        return tuple(operations)

    def simple_search(self, query_str):
        """Simple search over a database."""
        operations = self.parse_simple_query(query_str)
        import ipdb; ipdb.set_trace()