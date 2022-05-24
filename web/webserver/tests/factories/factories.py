import factory
import faker
from ...models import Taxons, AccessionNumbers, RecordDetails


def random_accession(prefix: str) -> str:
    return f"{prefix}{faker.Faker().random_int(min=100, max=10000)}"


class TaxonFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = Taxons

    taxon_id = factory.Faker('random_int', min=100, max=10000)


class AccessionFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = AccessionNumbers
        django_get_or_create = ('taxon_id',)

    taxon_id = factory.SubFactory(TaxonFactory)
    accession = factory.LazyAttribute(lambda _: random_accession('ERR'))
    experiment_accession = factory.LazyAttribute(lambda _: random_accession('ERX'))
    run_accession = accession
    sample_accession = factory.LazyAttribute(lambda _: random_accession('SAME'))
    secondary_sample_accession = factory.LazyAttribute(lambda _: random_accession('ERS'))
    accession_id = factory.LazyAttribute(lambda a: f"{a.experiment_accession}_{a.run_accession}_{a.sample_accession}")
    fastq_ftp = factory.LazyAttribute(lambda _: f"{faker.Faker().url('ftp')};{faker.Faker().url('ftp')}")
    # time_fetched = factory.Faker('iso8601')
    passed_filter = factory.Faker('boolean')

    filter_failed = factory.LazyAttribute(lambda a: factory.Faker(
            'words',
            nb=1,
            ext_word_list=[
                'library_strategy = WGS',
                'instrument_platform = ILLUMINA',
                'library_source = GENOMIC',
                'library_layout = PAIRED',
                'base_count size',
                'date acceptable'
            ]
        ) if not a.passed_filter else None)
    # waiting_since = factory.LazyAttribute(
    #     lambda a: factory.Faker('iso8601') if a.passed_filter else None
    # )
    assembly_result = factory.LazyAttribute(lambda a: faker.Faker().words(
        nb=1,
        ext_word_list=['success', 'fail']
    ) if a.passed_filter else None)
    assembly_report_url = factory.LazyAttribute(lambda a: faker.Faker().uri if a.passed_filter else None)
    assembled_genome_url = factory.LazyAttribute(
        lambda a: faker.Faker().uri() if a.assembly_result == 'success' else None
    )
