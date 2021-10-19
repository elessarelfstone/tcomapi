import luigi

from tasks.companies import (RCutUrlFile, rcut_entrepreneurs,
                             rcut_foreign_branches, rcut_joint_ventures,
                             rcut_legal_branches, rcut_legal_entities)

active_statuses = [39354, 39355, 39356]


class PrepareSgovRCutUrls(luigi.WrapperTask):
    def requires(self):
        yield RCutUrlFile(name=f'statgovkz_{rcut_legal_entities}', juridical_type=742679)
        yield RCutUrlFile(name=f'statgovkz_{rcut_joint_ventures}', juridical_type=742687)
        yield RCutUrlFile(name=f'statgovkz_{rcut_legal_branches}', juridical_type=742680)
        yield RCutUrlFile(name=f'statgovkz_{rcut_foreign_branches}', juridical_type=742684)
        yield RCutUrlFile(name=f'statgovkz_{rcut_entrepreneurs}', juridical_type=742681)


class PrepareSgovActiveRCutUrls(luigi.WrapperTask):
    def requires(self):
        yield RCutUrlFile(name=f'statgovkz_active_{rcut_legal_entities}',
                          statuses=active_statuses, juridical_type=742679)
        yield RCutUrlFile(name=f'statgovkz_active_{rcut_joint_ventures}',
                          statuses=active_statuses, juridical_type=742687)
        yield RCutUrlFile(name=f'statgovkz_active_{rcut_legal_branches}',
                          statuses=active_statuses, juridical_type=742680)
        yield RCutUrlFile(name=f'statgovkz_active_{rcut_foreign_branches}',
                          statuses=active_statuses, juridical_type=742684)
        yield RCutUrlFile(name=f'statgovkz_active_{rcut_entrepreneurs}',
                          statuses=active_statuses, juridical_type=742681)


if __name__ == '__main__':
    luigi.run()

