import factory
import faker
import django.conf.global_settings
from ...models import Taxons, Records, RecordDetails, AssemblyStatus

fake = faker.Faker(django.conf.global_settings.LANGUAGE_CODE)


def random_accession(prefix: str) -> str:
    return f"{prefix}{fake.random_int(min=100, max=10000)}"


class TaxonFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Taxons
        django_get_or_create = ('id',)

    id = factory.Faker('random_int', min=100, max=10000)


class RecordFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Records
        django_get_or_create = ('id',)

    class Params:
        filtered = factory.Trait(
            assembly_result=AssemblyStatus.WAITING.value,
            passed_filter=True,
            filter_failed=None,
            waiting_since=fake.date_time_this_month()
        )
        accepted = factory.Trait(
            assembly_result=AssemblyStatus.IN_PROGRESS.value,
            waiting_since=fake.date_time_between('-7d')
        )
        completed = factory.Trait(
            assembly_result=AssemblyStatus.FAIL.value,
            assembly_error_report_url=fake.uri,
            waiting_since=fake.date_time_this_month()
        )
        assembled = factory.Trait(
            assembly_result=AssemblyStatus.SUCCESS.value,
            assembled_genome_url=fake.uri,
            waiting_since=fake.date_time_this_month()
        )

    taxon = factory.SubFactory(TaxonFactory)
    accession = factory.LazyAttribute(lambda _: random_accession('ERR'))
    experiment_accession = factory.LazyAttribute(lambda _: random_accession('ERX'))
    run_accession = accession
    sample_accession = factory.LazyAttribute(lambda _: random_accession('SAME'))
    secondary_sample_accession = factory.LazyAttribute(lambda _: random_accession('ERS'))
    id = factory.LazyAttribute(lambda a: f"{a.experiment_accession}_{a.run_accession}_{a.sample_accession}")
    fastq_ftp = factory.LazyAttribute(lambda _: f"{fake.url(['ftp'])};{fake.url(['ftp'])}")

    @factory.lazy_attribute
    def time_fetched(self):
        return fake.date_time_this_year().isoformat()

    # Trait defaults
    passed_filter = False
    filter_failed = factory.LazyAttribute(lambda a: fake.word(
            ext_word_list=[
                'library_strategy = WGS',
                'instrument_platform = ILLUMINA',
                'library_source = GENOMIC',
                'library_layout = PAIRED',
                'base_count size',
                'date acceptable'
            ]
        ))
    assembly_result = AssemblyStatus.SKIPPED.value
    assembled_genome_url = None
    waiting_since = None

    # Traits
    filtered = factory.Sequence(lambda n: n % 10 > 0)
    accepted = factory.LazyAttributeSequence(lambda o, n: o.filtered and n % 5 == 0)
    completed = factory.LazyAttributeSequence(lambda o, n: o.accepted and n % 4 == 0)
    assembled = factory.LazyAttributeSequence(lambda o, n: o.accepted and n % 2 == 0)


class RecordDetailsFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = RecordDetails
        django_get_or_create = ('record_id',)


