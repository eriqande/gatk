package org.broadinstitute.hellbender.utils;

import htsjdk.tribble.TribbleException;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.broadinstitute.hellbender.engine.GATKPath;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class ReEncodeVCFFilesTest {

    @DataProvider
    public Object[][] vcfFilesToReEncode() {
        return new Object[][]{
                //NOTE: In addition to these files, there is one remote test file (testSelectVariants_SimpleSelection.vcf)
                // in the gcp staging area that will need to be updated.
                //
                // Test files that will get re-encoded as part of the 4.3 upgrade.
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/vcfexample2.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/complexExample1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/haploid-multisample.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/CEUtrioTest.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/vcfexample.forNoCallFiltering.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/multi-allelic-ordering.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/selectVariants.onePosition.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/vcf4.1.example.vcf"},
//                // note:this is v3.3 file and has to be re-encoded using SelectVariants
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/test.dup.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/261_S01_raw_variants_gvcf.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/multi-allelic.bi-allelicInGIH.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/tetra-diploid.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/tetraploid-multisample.vcf"},
//                {"src/test/resources/large/CalculateGenotypePosteriors/expectedCGP_testMissingPriors.vcf"},
//                {"src/test/resources/large/CalculateGenotypePosteriors/expectedCGP_testUsingDiscoveredAF.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testClusteredSnps.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testFilter1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testFilter2.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testFilterWithSeparateNames.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testGenotypeFilters1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testGenotypeFilters2.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testInvertFilter.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testInvertJexlFilter.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testMask1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testMask2.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testMask3.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testMask4.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testMaskReversed.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/filters/VariantFiltration/expected/testVariantFiltration_testNoAction.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/CalculateGenotypePosteriors/expectedCGP_testDefaultsWithPanel.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/CalculateGenotypePosteriors/expectedCGP_testFamilyPriors_chr1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/CalculateGenotypePosteriors/expectedCGP_testInputINDELs.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/CalculateGenotypePosteriors/expectedCGP_testNumRefWithPanel.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/CalculateGenotypePosteriors/overlappingVariants.expected.no_duplicates.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_maxIndelSize296.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_maxIndelSize342.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_notrim.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_notrim_split_multiallelics.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_split_multiallelics.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_left_align_hg38_split_multiallelics_keepOrigAC.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/LeftAlignAndTrimVariants/expected_split_with_AS_filters.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/ReblockGVCF/prod.chr20snippet.withRawMQ.expected.g.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/ReblockGVCF/testJustOneSample.expected.g.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/ReblockGVCF/testNonRefADCorrection.expected.g.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_ComplexSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_ComplexSelectionWithNonExistingSamples.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_Concordance.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_Discordance.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_DropAnnotations.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_DropAnnotationsSelectFisherStrand.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_DropAnnotationsSelectGQ.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_DropAnnotationsSelectRD.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_DropAnnotationsSelectRMSMAPQ.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_ExcludeSelectionID.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_ExcludeSelectionType.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_Haploid.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_InvertJexlSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_InvertMendelianViolationSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_InvertSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_KeepOriginalDP.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_KeepSelectionID.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MaxFilteredGenotypesSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MendelianViolationSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MinIndelLengthSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MultiAllelicAnnotationOrdering.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MultiAllelicExcludeNonVar.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_MultipleRecordsAtOnePosition.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_NoGTs.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_RemoveSingleSpanDelAlleleExNoVar.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_RepeatedLineSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SampleExclusionFromFileAndSeparateSample.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SampleExclusionJustFromExpression.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SampleExclusionJustFromFile.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SampleExclusionJustFromRegexExpression.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SimpleDiploid.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_SimpleSelection.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_TetraDiploid.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_Tetraploid.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_UnusedAlleleCCCT.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_UnusedAlleleCCCTEnv.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_UnusedAlleleCCCTTrim.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_UnusedAlleleCCCTTrimAltEnv.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_maxNOCALLnumber1.vcf"},
//                {"src/test/resources/org/broadinstitute/hellbender/tools/walkers/variantutils/SelectVariants/expected/testSelectVariants_maxNOCALLnumber2.vcf"},
        };
    }

    // This is not really a test, just masquerading as one. Reencodes the VCF files in the data provider.
    @Test(dataProvider = "vcfFilesToReEncode",enabled = false) // don't actually run this test during CI
    public void reencodeVCFFile(final String sourceVCFName) throws IOException {
        final Logger logger = LogManager.getLogger(this.getClass());
        final GATKPath sourceVCFPath = new GATKPath(sourceVCFName);
        final String tempFileName = sourceVCFPath.toPath().toFile().getName();
        final File tempVCFFile = IOUtils.createTempFile(tempFileName, (new GATKPath(sourceVCFName).getExtension().get()));
        try (final VCFFileReader reader = new VCFFileReader(sourceVCFPath.toPath(), false);
             final VariantContextWriter writer = new VariantContextWriterBuilder()
                     .setOutputPath(tempVCFFile.toPath())
                     .setOption(Options.ALLOW_MISSING_FIELDS_IN_HEADER)
                     .unsetOption(Options.INDEX_ON_THE_FLY)
                     .build()
        ) {
            final VCFHeader header = reader.getHeader();
            writer.writeHeader(header);
            for (final VariantContext v : reader.iterator().toList()) {
                VariantContext decoded;
                try {
                    decoded = v.fullyDecode(header, true);
                } catch (final TribbleException e) {
                    // Use original VC if decoding failed because of missing field in header
                    logger.warn(String.format("Failed to decode variant %s \n%s\n", e.getMessage(), v));
                    decoded = v;
                }
                writer.add(decoded);
            }
        }
        // Replace original file with re-encoded one
        FileUtils.copyFile(tempVCFFile, sourceVCFPath.toPath().toFile(), false);
    }
}
