package org.broadinstitute.hellbender.tools.walkers.vqsr;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.testng.annotations.Test;

/**
 * These tests are scaled down versions of GATK3 tests (the reduction coming from smaller query intervals),
 * and the expected results files were generated by running GATK on the same files with the same arguments.
 * In most cases, the GATK3 runs were done using modifications of the integration tests, as opposed to running
 * from the command line, in order to ensure that the initial state of the random number generator is the same
 * between versions. In all cases, the variants in the expected files are identical to those produced by GATK3,
 * though the VCF headers were hand modified to account for small differences in the metadata lines.
 */
public class ScikitLearnVariantTrainIntegrationTest extends CommandLineProgramTest {

    @Test
    public void test1kgp50Exomes() {
        final String[] arguments = {
                "-V", "/home/slee/working/vqsr/1kgp-50-exomes/resources/1kgp-50-exomes.sites_only.vcf.gz",
                "-O", "/home/slee/working/vqsr/1kgp-50-exomes/sklearn-test/sklearn.snps",
                "--python-script", "/home/slee/working/vqsr/1kgp-50-exomes/sklearn-test/sklearn.py",
                "--hyperparameters-json", "/home/slee/working/vqsr/1kgp-50-exomes/sklearn-test/sklearn.hyperparameters.json",
                "--trust-all-polymorphic",
                "--tranche"," 100.0",
                "-tranche", "99.95",
                "-tranche", "99.9",
                "-tranche", "99.8",
                "-tranche", "99.7",
                "-tranche", "99.6",
                "-tranche", "99.5",
                "-tranche", "99.4",
                "-tranche", "99.3",
                "-tranche", "99.0",
                "-tranche", "98.0",
                "-tranche", "97.0",
                "-tranche", "90.0",
                "-an", "FS",
                "-an", "ReadPosRankSum",
                "-an", "MQRankSum",
                "-an", "QD",
                "-an", "SOR",
                "-an", "MQ",
                "-mode", "SNP",
                "--sample-every-Nth-variant", "1",
                "--resource:hapmap,known=false,training=true,truth=true,prior=15", "/mnt/4AB658D7B658C4DB/working/ref/hapmap_3.3.hg38.vcf.gz",
//                "--resource:omni,known=false,training=true,truth=true,prior=12", "/mnt/4AB658D7B658C4DB/working/ref/1000G_omni2.5.hg38.vcf.gz",
//                "--resource:1000G,known=false,training=true,truth=false,prior=10", "/mnt/4AB658D7B658C4DB/working/ref/1000G_phase1.snps.high_confidence.hg38.vcf.gz",
//                "--resource:dbsnp,known=true,training=false,truth=false,prior=7", "/mnt/4AB658D7B658C4DB/working/ref/Homo_sapiens_assembly38.dbsnp138.vcf.gz",
                "--verbosity", "DEBUG"
        };
        runCommandLine(arguments);
    }
}
