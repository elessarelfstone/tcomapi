import luigi

from tasks.companies import (RCutUrlFile, rcut_entrepreneurs,
                             rcut_foreign_branches, rcut_joint_ventures,
                             rcut_legal_branches, rcut_legal_entities)


class PrepareSgovRCutUrls(luigi.WrapperTask):
    def requires(self):
        yield RCutUrlFile(name=rcut_legal_entities, juridical_type=742679)
        yield RCutUrlFile(name=rcut_joint_ventures, juridical_type=742687)
        yield RCutUrlFile(name=rcut_legal_branches, juridical_type=742680)
        yield RCutUrlFile(name=rcut_foreign_branches, juridical_type=742684)
        yield RCutUrlFile(name=rcut_entrepreneurs, juridical_type=742681)


if __name__ == '__main__':
    luigi.run()

