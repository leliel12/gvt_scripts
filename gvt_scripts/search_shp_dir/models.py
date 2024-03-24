import pathlib
import tempfile
import datetime
import re
import ast
import operator
import functools
from collections import defaultdict
import statistics as st
import math

import attrs

import dateutil.parser as dateparser

import peewee as pw

from ..utils import TEMP_DIR, Undefined


# =============================================================================
# DATETIME HELPERS MOCELS
# =============================================================================


def _utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


class DateableABC(pw.Model):

    created_at = pw.DateTimeField(default=_utc_now)
    updated_at = pw.DateTimeField(default=_utc_now)

    @classmethod
    def check_undefined(cls):
        """Check for undefined fields in the class and raise a TypeError if \
        found.

        """
        for aname in dir(cls):
            if getattr(cls, aname) is Undefined:
                raise TypeError(f"Undefined field {aname!r} in {cls!r}")


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

    md_directory = Undefined

    batch = pw.CharField()
    tarsize = pw.IntegerField()
    satellite = pw.CharField()
    sensorid = pw.CharField()
    acquisitio = pw.DateField(null=True)
    cloudperce = pw.FloatField()
    orbitid = pw.IntegerField()
    scenepath = pw.IntegerField()
    scenerow = pw.IntegerField()


class PRJMixin(DateableABC):

    md_directory = Undefined

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

    prj = Undefined

    name = pw.CharField()
    abbreviation = pw.CharField()
    direction = pw.CharField()
    unit_type = pw.CharField()
    unit_name = pw.CharField()
    unit_conversion_factor = pw.FloatField()


class JGWMixin(DateableABC):

    md_directory = Undefined

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

SIMPLE_QUERY_PATTERN = re.compile(
    r"(?P<arg>\w+)\s*" "(?P<op>!=|=|<=|<|>=|>|in|not in)\s*" "(?P<value>.+)"
)


@attrs.define(frozen=True)
class _SimpleOperation:
    """Simple query operation.

    Args:
        arg: Field name
        operation: Operation
        value: Operation value

    Raises:
        ValueError: If the operation is not supported

    """

    operation_methods = {
        "=": operator.eq,
        "!=": operator.ne,
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "in": operator.contains,
        "not in": lambda a, b: opetor.not_(operator.contains(a, b)),
    }

    arg: str = attrs.field(
        validator=attrs.validators.matches_re(r"^[a-zA-Z][a-zA-Z0-9_]*$")
    )
    operation: str = attrs.field(
        validator=attrs.validators.in_(operation_methods.keys())
    )
    value = attrs.field()

    @property
    def bind_method(self):
        """Operation method based on the current operation."""
        return self.operation_methods[self.operation]

    def bind(self, field):
        """Binds a field to the object, and returns peewee expression."""
        if field.name != self.arg:
            raise ValueError(f"Invalid bind field {field!r}")

        pyvalue = field.python_value(self.value)
        return self.bind_method(field, pyvalue)


def _model_field_factory(mixin, **fks):
    """Generate a model field factory function that takes a mixin and keyword \
    arguments for foreign keys.

    This function creates a model name based on the mixin name, and then
    creates a model with foreign key fields  based on the provided keyword
    arguments. It returns a field with the default value set to the created
    model.

    """

    model_name = mixin.__name__.removesuffix("Mixin")

    def _make(db):
        content = {}
        for fk, fk_attr_field in fks.items():
            content[fk] = pw.ForeignKeyField(getattr(db, fk_attr_field))

        model = type(model_name, (db.BaseModel, mixin), content)
        return model

    default = attrs.Factory(_make, takes_self=True)
    return attrs.field(init=False, repr=False, default=default)


@attrs.define(frozen=True)
class Database:

    db: pw.Database = attrs.field()
    BaseModel: pw.Model = attrs.field(init=False, repr=False)

    @BaseModel.default
    def _BaseModel_default(self):
        class BaseModel(DateableABC):

            class Meta:
                database = self.db

        return BaseModel

    MetaDataDirectory: pw.Model = _model_field_factory(MetaDataDirectoryMixin)
    DBFRecord: pw.Model = _model_field_factory(
        DBFRecordMixin, md_directory="MetaDataDirectory"
    )
    PRJ = _model_field_factory(PRJMixin, md_directory="MetaDataDirectory")
    CoordinateSystemAxisEntry = _model_field_factory(
        CoordinateSystemAxisEntryMixin, prj="PRJ"
    )
    JGW = _model_field_factory(JGWMixin, md_directory="MetaDataDirectory")

    @classmethod
    def from_url(cls, url):
        """Alternative constructor."""
        url = ":memory:" if url is None else url
        db = pw.SqliteDatabase(url)
        instance = cls(db=db)
        return instance

    def __attrs_post_init__(self):
        for model in self.models:
            model.check_undefined()
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
            cloudperce=float(cloudperce.replace(",", ".", 1)),
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

    def get_searcheable_fields(self):
        # this field types can be searched
        forbiden_ftypes = (
            pw.ForeignKeyField,
            pw.PrimaryKeyField,
            pw.AutoField,
        )
        # this field names cant be searched
        forbiden_fields = tuple(DateableABC._meta.fields)

        fields = {}
        for model in self.models:
            new_fields = {
                fn: fld
                for fn, fld in model._meta.fields.items()
                if not (
                    isinstance(fld, forbiden_ftypes) or fn in forbiden_fields
                )
            }
            fields.update(new_fields)
        return fields

    def searcheable_fields_by_models(self):
        fields = self.get_searcheable_fields()
        by_models = defaultdict(list)

        for fname, fld in fields.items():
            by_models[fld.model].append(fld)
        return dict(by_models)

    def field_info(self, field_name):
        field = self.get_searcheable_fields()[field_name]
        model = field.model
        query = tuple(model.select(field).distinct().scalars())
        field_type = field.field_type

        if isinstance(field, (pw.IntegerField, pw.FloatField)):
            stats = {
                "Count": len(query),
                "Uniques": len(set(query)),
                "Min": min(query),
                "Max": max(query),
                "NA": (math.nan in query or None in query),
                "Mean": st.mean(query),
                "Median": st.median(query),
                "Std": st.stdev(query) if len(query) > 1 else 0.,
            }
        elif isinstance(field, (pw.CharField, pw.TextField)):
            uniques = tuple(set(query))
            stats = {
                "Count": len(query),
                "Uniques": len(uniques),
                "Values": (
                    uniques[:10] + ("...",) if len(uniques) > 10 else uniques
                ),
            }
        else:
            stats = {"??": "??"}

        info = {
            "field_name": field_name,
            "model": model,
            "field_type": field.field_type,
            "stats": stats,
        }
        return info

    def parse_simple_query(self, query_str):

        operations = []
        for squery_str in query_str.split("&"):
            match = SIMPLE_QUERY_PATTERN.match(squery_str.strip())

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
                operations.append(_SimpleOperation(arg, operation, value))
            except Exception as err:
                raise ValueError(
                    f"Invalid query: {query_str!r}. Hint {squery_str!r}"
                ) from err

        return tuple(operations)

    def compile_query(self, operations):
        fields = self.get_searcheable_fields()
        exprs = []
        for opdef in operations:
            if opdef.arg not in fields:
                raise ValueError(f"Unknow field {opdef.arg!r}")

            field = fields[opdef.arg]
            exprs.append(opdef.bind(field))

        compiled = functools.reduce(operator.and_, exprs)
        return compiled

    def simple_search(self, query_str):
        """Performs a simple search over the database using the provided query string.

        The query string should consist of one or more conditions in the format:
        "field operator value" separated by "&". Supported operators are: "=", "!=",
        "<", "<=", ">", ">=", "in", "not in".

        Parameters
        ----------
        query_str : str
            The query string specifying the search conditions.

        Returns
        -------
        tuple
            A tuple of matching records from the database.

        Raises
        ------
        ValueError
            If the query string is invalid or contains unknown fields.

        Examples
        --------
        >>> db.simple_search("satellite = 'Landsat-8' & cloudperce <= 10")
        (<JGW: 1>, <JGW: 2>, <JGW: 3>)

        Notes
        -----
        The search is performed by joining multiple tables (JGW, MetaDataDirectory,
        DBFRecord, PRJ, CoordinateSystemAxisEntry) and applying the specified
        conditions to filter the records. The resulting records are returned as a
        tuple of distinct JGW objects.

        The function first parses the query string into a list of `_SimpleOperation`
        objects using the `parse_simple_query` method. It then compiles the operations
        into a peewee expression using the `compile_query` method. Finally, it
        constructs a peewee query by joining the necessary tables, applying the
        compiled expression as a filter, and returning the distinct JGW records.

        See Also
        --------
        parse_simple_query : Parse the query string into `_SimpleOperation` objects.
        compile_query : Compile the query operations into a peewee expression.
        """
        operations = self.parse_simple_query(query_str)
        expr = self.compile_query(operations)

        query = (
            (
                self.JGW.select()
                .join(self.MetaDataDirectory)
                .join(self.DBFRecord)
                .switch(self.MetaDataDirectory)
                .join(self.PRJ)
                .join(self.CoordinateSystemAxisEntry)
            )
            .where(expr)
            .distinct()
        )

        return tuple(query)


# =============================================================================
# FUNCTIONS
# =============================================================================


def model_to_dict(model_obj, visited=None, upper_level_model=None):
    """Recursively converts a Peewee model object and its related objects to
    a dictionary.

    Parameters
    ----------
    model_obj : Peewee Model
        The model object to convert to a dictionary.
    visited : set, optional
        A set to keep track of visited objects to avoid circular references.
    upper_level_model : set, optional
        A set to keep track of upper-level models to avoid revisiting them.

    Returns
    -------
    dict
        A dictionary representation of the model object and its related objects.

    """

    visited = set() if visited is None else visited
    upper_level_model = (
        set() if upper_level_model is None else upper_level_model
    )

    if model_obj in visited or type(model_obj) in upper_level_model:
        return None

    visited.add(model_obj)
    upper_level_model.add(type(model_obj))

    related_objects = {}

    # Convert the rest of the attributes to a dictionary
    data = dict(model_obj.__data__)

    # Get attributes with normal references from the model
    for fkfield in model_obj._meta.refs:
        # Avoiding circular references and revisiting upper-level models
        related_model = getattr(model_obj, fkfield.name)
        related_data = model_to_dict(related_model, visited, upper_level_model)
        if related_data is not None:
            data[fkfield.name] = related_data

    # Process back references
    for brmodel, brfields in model_obj._meta.model_backrefs.items():
        # Avoid revisiting upper-level models
        if brmodel in upper_level_model:
            continue

        backrefs = []
        for brfield in brfields:
            for refmodel in getattr(model_obj, brfield.backref):
                # Recursively convert back-referenced models to dictionaries
                refmodel_data = model_to_dict(
                    refmodel, visited, upper_level_model
                )
                if refmodel_data is not None:
                    backrefs.append(refmodel_data)

        if backrefs:
            data[brmodel.__name__] = backrefs

    return data


def records_as_list(records):
    rlist = []
    for model in records:
        rlist.append(model_to_dict(model))
    return rlist
