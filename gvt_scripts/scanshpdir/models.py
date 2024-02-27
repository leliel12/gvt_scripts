import pathlib
import tempfile
import datetime

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

    def store_mdir(self, mdir):
        mdir = pathlib.Path(mdir)
        reg = self.MetaDataDirectory(
            path_str=str(mdir), date_str=mdir.parent.name
        )
        reg.save()
        return reg

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
        mdir = pathlib.Path(mdir)
        mdir_reg = self.MetaDataDirectory.get(date_str=mdir.parent.name)

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
